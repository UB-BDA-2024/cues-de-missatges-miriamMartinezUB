import json

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from shared.database import SessionLocal
from shared.publisher import Publisher
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.elasticsearch_client import ElasticsearchClient
from shared.sensors.repository import DataCommand
from shared.timescale import Timescale
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
def get_sensors_near(latitude: float, longitude: float, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    # Add your get near call to the repository here
    # return repository.get_sensors_near(db=db, mongodb=mongodb_client, latitude=latitude, longitude=longitude)
    return []

# 🙋🏽‍♀️ Add here the route to search sensors by query to Elasticsearch
# Parameters:
# - query: string to search
# - size (optional): number of results to return
# - search_type (optional): type of search to perform
# - db: database session
# - mongodb_client: mongodb client
@router.get("/search")
def search_sensors(query: str, size: int = 10, search_type: str = "match", db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    #Add your search call to the repository here
    #return repository.search_sensors(db=db, mongodb=mongodb_client, query=query, size=size, search_type=search_type)
    return []

@router.get("")
def get_sensors(db: Session = Depends(get_db)):
    return repository.get_sensors(db)


# 🙋🏽‍♀️ Add here the route to create a sensor
@router.post("")
def create_sensor(sensor: schemas.SensorCreate, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    #The creation of the sensor should be sync (without queue)
    db_sensor = repository.get_sensor_by_name(db, sensor.name)
    if db_sensor:
        raise HTTPException(
            status_code=400, detail="Sensor with same name already registered")
    sensor_created = repository.create_sensor(db, mongodb_client, sensor)
    return sensor_created


@router.get("/{sensor_id}")
def get_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return db_sensor

# 🙋🏽‍♀️ Add here the route to delete a sensor
@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return repository.delete_sensor(db=db, sensor_id=sensor_id)

# 🙋🏽‍♀️ Add here the route to update a sensor


@router.post("/{sensor_id}/data")
def record_data(sensor_id: int, data: schemas.SensorData, db: Session = Depends(get_db), timescale: Timescale = Depends(get_timescale), redis_client: RedisClient = Depends(get_redis_client)):
    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    # We need to retrieve the query params from, to and bucket from the request
    # Publish here the data to the queue
    new_var = repository.record_data(
        redis=redis_client, timescale=timescale, sensor_id=sensor_id, data=data)
    return new_var

# 🙋🏽‍♀️ Add here the route to get data from a sensor
@router.get("/{sensor_id}/data")
def get_data(
        sensor_id: int,
        r: Request,
        db: Session = Depends(get_db),
        redis_client: RedisClient = Depends(get_redis_client),
        timescale: Timescale = Depends(get_timescale)):

    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    # Get the from, to and bucket from the request
    datacommand = DataCommand(
        r.query_params['from'], r.query_params['to'], r.query_params['bucket'])
    return repository.get_data(timescale=timescale, sensor_id=sensor_id, dataCommand=datacommand)


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