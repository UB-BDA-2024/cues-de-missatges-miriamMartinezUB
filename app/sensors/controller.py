import json

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from fastapi import Query

from shared.database import SessionLocal
from shared.publisher import Publisher
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.elasticsearch_client import ElasticsearchClient
from shared.sensors.exceptions import NotCompatible
from shared.sensors.repository import DataCommand
from shared.timescale import Timescale
from shared.cassandra_client import CassandraClient
from shared.sensors import repository, schemas

_SENSORS = 'sensors'

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_timescale():
    ts = Timescale()
    ts.create_table()
    try:
        yield ts
    finally:
        ts.close()


# Dependency to get redis client
def get_redis_client():
    redis = RedisClient(host="redis")
    try:
        yield redis
    finally:
        redis.close()


# Dependency to get mongodb client
def get_mongodb_client():
    mongodb = MongoDBClient(host="mongodb")
    mongodb.getDatabase(_SENSORS)
    try:
        yield mongodb
    finally:
        mongodb.close()


# Dependency to get elastic_search client
def get_elastic_search():
    es = ElasticsearchClient(host="elasticsearch")
    try:
        yield es
    finally:
        es.close()


# Dependency to get cassandra client
def get_cassandra_client():
    cassandra = CassandraClient(hosts=["cassandra"])
    cassandra.create_tables()
    try:
        yield cassandra
    finally:
        cassandra.close()


publisher = Publisher()

router = APIRouter(
    prefix="/sensors",
    responses={404: {"description": "Not found"}},
    tags=["sensors"],
)


# 🙋🏽‍♀️ Add here the route to get a list of sensors near to a given location
@router.get("/near")
def get_sensors_near(latitude: float, longitude: float, radius: float, db: Session = Depends(get_db),
                     redis_client: RedisClient = Depends(get_redis_client),
                     mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    return repository.get_sensors_near(db=db, mongo_client=mongodb_client, redis=redis_client, latitude=latitude,
                                       longitude=longitude, radius=radius)


# 🙋🏽‍♀️ Add here the route to search sensors by query to Elasticsearch
# Parameters:
# - query: string to search
# - size (optional): number of results to return
# - search_type (optional): type of search to perform
# - db: database session
# - mongodb_client: mongodb client
@router.get("/search")
def search_sensors(query: str, size: int = 10, search_type: str = "match", db: Session = Depends(get_db),
                   mongodb_client: MongoDBClient = Depends(get_mongodb_client),
                   es: ElasticsearchClient = Depends(get_elastic_search)):
    try:
        return repository.search_sensors(db=db, mongo_client=mongodb_client, es=es, query=query, size=size,
                                         search_type=search_type)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Sensors not found")



@router.get("/temperature/values")
def get_temperature_values(db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client),
                           cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_temperature_values(db=db, mongo_client=mongodb_client, cassandra=cassandra_client)


@router.get("/quantity_by_type")
def get_sensors_quantity(cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_sensors_quantity(cassandra=cassandra_client)


@router.get("/low_battery")
def get_low_battery_sensors(db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client),
                            cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_low_battery_sensors(db=db, mongo_client=mongodb_client, cassandra=cassandra_client)

@router.get("")
def get_sensors(db: Session = Depends(get_db)):
    return repository.get_sensors(db)


# 🙋🏽‍♀️ Add here the route to create a sensor
@router.post("")
def create_sensor(sensor: schemas.SensorCreate, db: Session = Depends(get_db),
                  mongodb_client: MongoDBClient = Depends(get_mongodb_client),
                  es: ElasticsearchClient = Depends(get_elastic_search)):
    db_sensor = repository.get_sensor_by_name(db, sensor.name)
    if db_sensor:
        raise HTTPException(status_code=400, detail="Sensor with same name already registered")
    return repository.create_sensor(mongo_client=mongodb_client, db=db, sensor=sensor, es=es)



@router.get("/{sensor_id}")
def get_sensor(sensor_id: int, db: Session = Depends(get_db),
               mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    try:
        return repository.get_sensor_schema(mongo_client=mongodb_client, db=db, sensor_id=sensor_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Sensor not found")


# 🙋🏽‍♀️ Add here the route to delete a sensor
@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: int, db: Session = Depends(get_db),
                  redis_client: RedisClient = Depends(get_redis_client),
                  mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    try:
        return repository.delete_sensor(db=db, mongo_client=mongodb_client, redis=redis_client, sensor_id=sensor_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Sensor not found")


# 🙋🏽‍♀️ Add here the route to update a sensor


@router.post("/{sensor_id}/data")
def record_data(sensor_id: int, data: schemas.SensorDataTemperature | schemas.SensorDataVelocity,
                db: Session = Depends(get_db), redis_client: RedisClient = Depends(get_redis_client),
                mongodb_client: MongoDBClient = Depends(get_mongodb_client),
                cassandra_client: CassandraClient = Depends(get_cassandra_client),
                timescale: Timescale = Depends(get_timescale)):
    try:
        return repository.record_data(timescale=timescale, redis=redis_client, mongo_client=mongodb_client,
                                      db=db, cassandra=cassandra_client,
                                      sensor_id=sensor_id,
                                      data=data)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Sensor not found")
    except NotCompatible as e:
        raise HTTPException(status_code=409, detail=e.message)

# 🙋🏽‍♀️ Add here the route to get data from a sensor
@router.get("/{sensor_id}/data")
def get_data(
        sensor_id: int,
        r: Request,
        db: Session = Depends(get_db),
        mongodb_client: MongoDBClient = Depends(get_mongodb_client),
        timescale: Timescale = Depends(get_timescale)):
    try:
        # Get the from, to and bucket from the request
        data_command = DataCommand(
            r.query_params['from'], r.query_params['to'], r.query_params['bucket'])

        return repository.get_data(timescale=timescale,
                                   mongo_client=mongodb_client, db=db,
                                   sensor_id=sensor_id,dataCommand=data_command)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Sensor not found")
    except ValueError:
        raise HTTPException(status_code=404, detail="Data not found")
    except TypeError:
        raise HTTPException(status_code=409, detail="Conflict - This type of sensor doesn't exist")
    except NotCompatible as e:
        raise HTTPException(status_code=409, detail=e.message)


class ExamplePayload():
    def __init__(self, example):
        self.example = example

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
@router.post("/exemple/queue")
def exemple_queue():
    # Publish here the data to the queue
    publisher.publish(ExamplePayload("holaaaaa"))
    return {"message": "Data published to the queue"}