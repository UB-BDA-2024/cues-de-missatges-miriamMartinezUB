from pydantic import BaseModel


class Sensor(BaseModel):
    id: int
    name: str
    latitude: float
    longitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str
    description: str

    class Config:
        orm_mode = True


class SensorTemperature(Sensor):
    last_seen: str
    battery_level: float
    temperature: float
    humidity: float


class SensorVelocity(Sensor):
    last_seen: str
    battery_level: float
    velocity: float


class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str
    description: str


class SensorData(BaseModel):
    battery_level: float
    last_seen: str

    class Config:
        orm_mode = True


class SensorDataTemperature(SensorData):
    temperature: float
    humidity: float


class SensorDataVelocity(SensorData):
    velocity: float


class SensorDataSearch(BaseModel):
    name: str
    type: str
    description: str


class TemperatureValues(BaseModel):
    max_temperature: float
    min_temperature: float
    average_temperature: float


class SensorsSetTemperatureItem(Sensor):
    values: TemperatureValues


class SensorsSetLowBatteryItem(Sensor):
    battery_level: float


class SensorsSetQuantityItem(BaseModel):
    type: str
    quantity: int


class SensorSet(BaseModel):
    sensors: list[SensorsSetTemperatureItem | SensorsSetQuantityItem | SensorsSetLowBatteryItem]