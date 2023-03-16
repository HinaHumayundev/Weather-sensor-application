
from sqlalchemy import  Column, Integer, Float, Enum, DateTime
from database import Base

class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True)
    external_temperature_c = Column(Float)
    wind_speed_unmuted_m_s = Column(Float)
    wind_speed_m_s = Column(Float)
    wind_direction_degrees = Column(Integer)
    wind_direction_compass = Column(
        Enum('W', 'WZW', 'ZW', 'WNW', 'ZZW', 'NW', 'NNW', 'Z', 'ZZO', 'ZO', 'OZO', 'O', 'NNO', 'ONO', 'NO', 'N'))
    radiation_intensity_unmuted_w_m2 = Column(Float)
    radiation_intensity_w_m2 = Column(Float)
    standard_radiation_intensity_w_m2 = Column(Float)
    radiation_sum_j_cm2 = Column(Float)
    radiation_from_plant_w_m2 = Column(Float)
    precipitation = Column(Float)
    relative_humidity_perc = Column(Float)
    moisture_deficit_g_kg = Column(Float)
    moisture_deficit_g_m3 = Column(Float)
    dew_point_temperature_c = Column(Float)
    abs_humidity_g_kg = Column(Float)
    enthalpy_kj_kg = Column(Float)
    enthalpy_kj_m3 = Column(Float)
    atmospheric_pressure_hpa = Column(Float)
    status_meteo_station = Column(Enum('Actief', 'Inactief'))
    status_meteo_station_communication = Column(Enum('Online', 'Offline'))
    timestamp = Column(DateTime)