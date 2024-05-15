from fastapi.testclient import TestClient
import pytest
from app.main import app
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.cassandra_client import CassandraClient

client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
def clear_dbs():
     from shared.database import engine
     from shared.sensors import models
     models.Base.metadata.drop_all(bind=engine)
     models.Base.metadata.create_all(bind=engine)
     redis = RedisClient(host="redis")
     redis.clearAll()
     redis.close()
     mongo = MongoDBClient(host="mongodb")
     mongo.clearDb("sensors")
     mongo.close()

     cassandra = CassandraClient(["cassandra"])
     cassandra.get_session().execute("DROP KEYSPACE IF EXISTS sensor")
     cassandra.close()




#TODO ADD all your tests in test_*.py files:


# añadir todos los tests de todas las practicas se haga o no la parte de cola de mensajes pero con diferentes ficheros i
# actualizamos los tests antiguos si es necesario correr los test solo levantando lo necesario, o postgress o redis etc