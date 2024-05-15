import json
from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from shared.cassandra_client import CassandraClient

from .exceptions import NotCompatible
from .schemas import SensorDataSearch, SensorSet, TemperatureValues, SensorsSetTemperatureItem, SensorsSetQuantityItem, \
    SensorsSetLowBatteryItem
from shared.elasticsearch_client import ElasticsearchClient

from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.timescale import Timescale
_SENSORS = 'sensors'

class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise FileNotFoundError
    return db_sensor


def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(mongo_client: MongoDBClient, db: Session, sensor: schemas.SensorCreate,
                  es: ElasticsearchClient) -> schemas.Sensor:
    name = sensor.name
    # Create in mySQL
    db_sensor = _add_sensor_to_postgres(db, sensor)
    # Create in MongoDb
    collection = mongo_client.getCollection(_SENSORS)
    collection.insert_one(sensor.dict())
    # Create in ElasticSearch
    es_data = SensorDataSearch(name=name, type=sensor.type, description=sensor.description)
    es.index_document(_SENSORS, es_data.dict())
    return _get_sensor_from_db_sensor_and_sensor_create(db_sensor=db_sensor, sensor_create=sensor)


def record_data(timescale: Timescale, redis: RedisClient, mongo_client: MongoDBClient, db: Session,
                sensor_id: int, cassandra: CassandraClient,
                data: schemas.SensorDataTemperature | schemas.SensorDataVelocity) -> schemas.Sensor:
    redis.set(sensor_id, data.json())
    sensor = _from_id_and_data_to_sensor(
        sensor=_get_sensor_from_sensor_id(mongo_client=mongo_client, db=db, sensor_id=sensor_id),
        data=data)

    query = """
            INSERT INTO sensor_data (time,name, temperature, humidity, velocity, battery_level, last_seen) 
            VALUES (%s,%s, %s, %s, %s, %s, %s)
        """
    time = datetime.utcnow().isoformat()
    if sensor.type == 'Temperatura':
        values = (time, sensor.name, data.temperature, data.humidity, None, data.battery_level, data.last_seen)
        _insert_temperature_data(cassandra=cassandra, time=time, name=sensor.name, temperature=data.temperature)
    elif sensor.type == 'Velocitat':
        values = (time, sensor.name, None, None, data.velocity, data.battery_level, data.last_seen)
    else:
        raise TypeError
    _insert_type_sensor(cassandra=cassandra, sensor_id=sensor_id, sensor_type=sensor.type)
    _insert_battery_level(cassandra=cassandra, name=sensor.name, battery_level=data.battery_level)
    timescale.execute(query, values)
    return sensor

def get_data(timescale: Timescale, mongo_client: MongoDBClient, db: Session, sensor_id: int, dataCommand: DataCommand):
    view = _getView(dataCommand.bucket)
    sensor = _get_sensor_from_sensor_id(db=db, mongo_client=mongo_client, sensor_id=sensor_id)
    if sensor.type == 'Temperatura':
        query = f"SELECT time, temperature, humidity, battery_level, last_seen  FROM sensor_data WHERE name = {sensor.name} AND bucket >= '{dataCommand.from_time}' AND bucket <= '{dataCommand.to_time}'"
    elif sensor.type == 'Velocitat':
        query = f"SELECT time, velocity battery_level, last_seen  FROM sensor_data WHERE name = {sensor.name} AND bucket >= '{dataCommand.from_time}' AND bucket <= '{dataCommand.to_time}'"
    else:
        raise TypeError
    timescale.execute(query)
    results = timescale.get_cursor().fetchall()
    return results


def delete_sensor(db: Session, redis: RedisClient, mongo_client: MongoDBClient, sensor_id: int):
    db_sensor = get_sensor(sensor_id=sensor_id, db=db)
    # Delete from mongo
    collection = mongo_client.getCollection(_SENSORS)
    collection.delete_one({"name": db_sensor.name})
    # Delete from redis
    redis.delete(sensor_id)
    # Delete from SQL
    db.delete(db_sensor)
    db.commit()
    return db_sensor


def get_sensors_near(db: Session, redis: RedisClient, mongo_client: MongoDBClient, latitude: float, longitude: float,
                     radius: float) -> \
        List[schemas.Sensor]:
    sensors = []
    collection = mongo_client.getCollection(_SENSORS)
    radius_in_degrees = radius / 111.12
    sensors_dicts = collection.find({
        'latitude': {'$gte': latitude - radius_in_degrees, '$lte': latitude + radius_in_degrees},
        'longitude': {'$gte': longitude - radius_in_degrees, '$lte': longitude + radius_in_degrees}
    })
    for sensor_dict in sensors_dicts:
        sensor_create = schemas.SensorCreate(**sensor_dict)
        db_sensor = db.query(models.Sensor).filter(models.Sensor.name == sensor_create.name).first()
        sensor = _get_sensor_from_db_sensor_and_sensor_create(db_sensor=db_sensor, sensor_create=sensor_create)
        try:
            sensor_data = _get_data(redis=redis, sensor_id=db_sensor.id, type=sensor.type)
        except ValueError:
            sensors.append(sensor)
        else:
            sensor_with_data = _from_id_and_data_to_sensor(sensor=sensor, data=sensor_data)
            sensors.append(sensor_with_data)
    return sensors


def search_sensors(db: Session, mongo_client: MongoDBClient, es: ElasticsearchClient, query: str, size: int = 10,
                   search_type: str = "match"):
    search_query = _get_query(query, size, search_type)
    result_search = es.search(_SENSORS, search_query)
    results = []
    for hit in result_search['hits']['hits']:
        results.append(
            _get_sensor_from_sensor_name(sensor_name=hit['_source']['name'], db=db, mongo_client=mongo_client))
    return results


def get_temperature_values(db: Session, mongo_client: MongoDBClient, cassandra: CassandraClient) -> SensorSet:
    sensor_set_items = []
    collection = mongo_client.getCollection(_SENSORS)
    sensors_dicts = collection.find({"type": "Temperatura"})
    for sensor_dict in sensors_dicts:
        sensor = schemas.SensorCreate(**sensor_dict)
        query = """
            SELECT MAX(temperature), MIN(temperature), AVG(temperature)
            FROM temperature_data
            WHERE name  = %s
            ALLOW FILTERING;
        """
        result = cassandra.execute(query, (sensor.name,))
        max_temperature, min_temperature, avg_temperature = result.one()
        values = TemperatureValues(max_temperature=max_temperature, min_temperature=min_temperature,
                                   average_temperature=avg_temperature)
        db_sensor = get_sensor_by_name(db=db, name=sensor.name)
        sensor_set_items.append(
            SensorsSetTemperatureItem(id=db_sensor.id, name=sensor.name,
                                      latitude=sensor.latitude,
                                      longitude=sensor.longitude,
                                      type=sensor.type,
                                      mac_address=sensor.mac_address,
                                      manufacturer=sensor.manufacturer,
                                      model=sensor.model,
                                      serie_number=sensor.serie_number,
                                      firmware_version=sensor.firmware_version,
                                      description=sensor.description, values=values))

    return SensorSet(sensors=sensor_set_items)


def get_sensors_quantity(cassandra: CassandraClient) -> SensorSet:
    sensor_set_items = []
    query = """
        SELECT sensor_type, COUNT(*) AS count
        FROM type_sensor
        GROUP BY sensor_type
        ALLOW FILTERING;
    """
    result = cassandra.execute(query)
    for item in result:
        sensor_set_items.append(SensorsSetQuantityItem(quantity=item[1], type=item[0]))
    return SensorSet(sensors=sensor_set_items)


def get_low_battery_sensors(db: Session, mongo_client: MongoDBClient, cassandra: CassandraClient) -> SensorSet:
    sensor_set_items = []
    query = """
        SELECT name,battery_level 
        FROM low_battery
        WHERE battery_level <= 0.2
        ALLOW FILTERING;
    """
    result = cassandra.execute(query)
    for item in result:
        name = item[0]
        sensor = _get_sensor_from_sensor_name(db=db, mongo_client=mongo_client, sensor_name=name)
        sensor_set_items.append(SensorsSetLowBatteryItem(id=sensor.id, name=sensor.name,
                                                         latitude=sensor.latitude,
                                                         longitude=sensor.longitude,
                                                         type=sensor.type,
                                                         mac_address=sensor.mac_address,
                                                         manufacturer=sensor.manufacturer,
                                                         model=sensor.model,
                                                         serie_number=sensor.serie_number,
                                                         firmware_version=sensor.firmware_version,
                                                         description=sensor.description, battery_level=item[1]))

    return SensorSet(sensors=sensor_set_items)


def _insert_temperature_data(cassandra: CassandraClient, time, name, temperature):
    insert_query = """
        INSERT INTO temperature_data (time, name, temperature)
        VALUES (%s, %s, %s)
    """
    cassandra.execute(insert_query, (time, name, temperature))


def _insert_type_sensor(cassandra: CassandraClient, sensor_id: int, sensor_type: str):
    insert_query = """
            INSERT INTO type_sensor (sensor_type, id)
            VALUES (%s, %s)
        """
    cassandra.execute(insert_query, (sensor_type, sensor_id))


def _insert_battery_level(cassandra: CassandraClient, name: str, battery_level: float):
    insert_query = """
            INSERT INTO low_battery (battery_level, name)
            VALUES (%s, %s)
        """
    cassandra.execute(insert_query, (battery_level, name))


def _get_query(query: str, size: int = 10, search_type: str = "match"):
    query_dict = json.loads(query)
    if search_type == "similar":
        return {
            'query': {
                'match': {
                    list(query_dict.keys())[0]: {
                        'query': list(query_dict.values())[0],
                        'fuzziness': "auto",
                        'operator': "and"
                    }
                }
            },
            'size': size,
            'from': 0
        }
    else:
        if search_type == "match":
            query = 'match_phrase'
        else:
            query = 'match_phrase_prefix'
        return {
            'query': {
                query: query_dict
            },
            'size': size,
            'from': 0
        }


def _get_data(redis: RedisClient, sensor_id: int, type: str) -> schemas.SensorData | None:
    redis_data = redis.get(sensor_id)
    if redis_data is None:
        raise ValueError
    # Parse json to dict
    data_dict = json.loads(redis_data)
    # get Sensor Data from dict
    match type:
        case 'Temperatura':
            sensor_data = schemas.SensorDataTemperature(**data_dict)
        case 'Velocitat':
            sensor_data = schemas.SensorDataVelocity(**data_dict)
        case _:
            raise TypeError
    return sensor_data


def _from_id_and_data_to_sensor(sensor: schemas.Sensor, data: schemas.SensorData) -> schemas.Sensor:
    match sensor.type:
        case 'Temperatura':
            if type(data) is not schemas.SensorDataTemperature:
                raise NotCompatible(
                    "Conflict - The sensor with the specific id is of type temperature and you give data of velocity sensor")

            return schemas.SensorTemperature(id=sensor.id, name=sensor.name, latitude=sensor.latitude,
                                             longitude=sensor.longitude, type=sensor.type,
                                             mac_address=sensor.name, manufacturer=sensor.manufacturer,
                                             model=sensor.model, serie_number=sensor.serie_number,
                                             firmware_version=sensor.firmware_version, description=sensor.description,
                                             last_seen=data.last_seen, battery_level=data.battery_level,
                                             temperature=data.temperature, humidity=data.humidity)
        case 'Velocitat':
            if type(data) is not schemas.SensorDataVelocity:
                raise NotCompatible(
                    "Conflict - The sensor with the specific id is of type velocity and you give data of temperature sensor")
            return schemas.SensorVelocity(id=sensor.id, name=sensor.name, latitude=sensor.latitude,
                                          longitude=sensor.longitude, type=sensor.type,
                                          mac_address=sensor.name, manufacturer=sensor.manufacturer,
                                          model=sensor.model, serie_number=sensor.serie_number,
                                          firmware_version=sensor.firmware_version, description=sensor.description,
                                          last_seen=data.last_seen, battery_level=data.battery_level,
                                          velocity=data.velocity)
        case _:
            raise TypeError


def _get_sensor_from_sensor_id(db: Session, mongo_client: MongoDBClient, sensor_id: int) -> schemas.Sensor:
    db_sensor = get_sensor(sensor_id=sensor_id, db=db)
    collection = mongo_client.getCollection(_SENSORS)
    sensor_dict = collection.find_one({"name": db_sensor.name})
    sensor_create = schemas.SensorCreate(**sensor_dict)
    return _get_sensor_from_db_sensor_and_sensor_create(db_sensor=db_sensor, sensor_create=sensor_create)


def _get_sensor_from_sensor_name(db: Session, mongo_client: MongoDBClient, sensor_name: str) -> schemas.Sensor:
    db_sensor = get_sensor_by_name(db, sensor_name)
    collection = mongo_client.getCollection(_SENSORS)
    sensor_dict = collection.find_one({"name": sensor_name})
    sensor_create = schemas.SensorCreate(**sensor_dict)
    return _get_sensor_from_db_sensor_and_sensor_create(db_sensor=db_sensor, sensor_create=sensor_create)


def _get_sensor_from_db_sensor_and_sensor_create(db_sensor: models.Sensor,
                                                 sensor_create: schemas.SensorCreate) -> schemas.Sensor:
    return schemas.Sensor(id=db_sensor.id, name=sensor_create.name,
                          latitude=sensor_create.latitude,
                          longitude=sensor_create.longitude,
                          type=sensor_create.type,
                          mac_address=sensor_create.mac_address,
                          manufacturer=sensor_create.manufacturer,
                          model=sensor_create.model,
                          serie_number=sensor_create.serie_number,
                          firmware_version=sensor_create.firmware_version,
                          description=sensor_create.description)


def _add_sensor_to_postgres(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    date = datetime.now()

    db_sensor = models.Sensor(name=sensor.name, joined_at=date)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    return db_sensor


def _getView(bucket: str) -> str:
    if bucket == 'year':
        return 'sensor_data_yearly'
    if bucket == 'month':
        return 'sensor_data_monthly'
    if bucket == 'week':
        return 'sensor_data_weekly'
    if bucket == 'day':
        return 'sensor_data_daily'
    elif bucket == 'hour':
        return 'sensor_data_hourly'
    else:
        raise ValueError("Invalid bucket size")