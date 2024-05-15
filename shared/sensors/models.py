import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String

from ..database import Base


class Sensor(Base):
    __tablename__ = "sensors"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    joined_at = Column(DateTime, default=datetime.datetime.utcnow)


class SensorData(Base):
    __tablename__ = "sensor_data"
    time = Column(DateTime, primary_key=True, index=True)
    name = Column(String, index=True)
    temperature = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    velocity = Column(Float, nullable=True)
    battery_level = Column(Float)
    last_seen = Column(DateTime, default=datetime.datetime.utcnow)