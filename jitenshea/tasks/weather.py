# coding: utf-8

"""Luigi tasks dedicated to retrieve weather data for some cities
"""

import os
import json
from datetime import datetime as dt

import requests

import pandas as pd

import luigi
from luigi.format import UTF8, MixedUnicodeBytes

from jitenshea import config
# from jitenshea.iodb import db


DATADIR = 'datarepo/weather'

OPENWEATHER_URL = 'http://api.openweathermap.org/data/2.5'

# OpenWeather City ids from http://bulk.openweathermap.org/sample/city.list.json.gz
CITY_ID = {'bordeaux': 3031582,
           'lyon': 2996944}

# URL = "http://www.prevision-meteo.ch/services/json/lat={lat}lng={lon}"
# Note: the coordinates come from Nominatim http://nominatim.openstreetmap.org/
# BORDEAUX_COORD = {"lat": 44.841225,
#                   "lon": -0.5800364}
# LYON_COORD = {"lat": 45.7578137,
#               "lon": 4.8320114}


def weather(city, datatype):
    """Get the weather (current or forecast) from the OpenWeatherMap API.

    Parameters
    ----------
    city : str
    datatype : datetime.datetime
    
    Returns
    -------
    Response object
    """
    if datatype not in ('weather', 'forecast'):
        raise ValueError("'{}' weather API datatype not supported.".format(datatype))
    url = "/".join([OPENWEATHER_URL, datatype])
    query_params = {"units": "metric",
                    "id": CITY_ID[city],
                    "appid": config['weather']['openweather_appid']}
    resp = requests.get(url, query_params)
    resp.raise_for_status()
    return resp


class CurrentCityWeatherJson(luigi.Task):
    """Luigi task for getting weather information for a specific place in a
    *json* file

    :warning: Need the definition of a API key stored in
    `jitenshea/config.ini`, see [OpenWeather
    website](https://openweathermap.org/api) for details
        
    Inherits from ̀luigi.Task`
        
    Attributes
    ----------
    timestamp : datetime.datetime
    city : str
    """
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=10)
    city = luigi.Parameter()

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime("%HH%M") # 16H35
        path = os.path.join(DATADIR, self.city, '{year}', '{month:02d}', '{day:02d}',
                            'current', '{ts}.json')
        return luigi.LocalTarget(path.format(year=year, month=month, day=day, ts=ts),
                                 format=MixedUnicodeBytes)

    def run(self):
        resp = weather(self.city, 'weather')
        # just try the json conversion
        _ = resp.json()
        with self.output().open('w') as fobj:
            fobj.write(resp.content)


class ForecastCityWeatherJson(luigi.Task):
    """Luigi task for getting weather forecasts at a specific date and place in
    a *json* file

    :warning: Need the definition of a API key stored in
    `jitenshea/config.ini`, see [OpenWeather
    website](https://openweathermap.org/api) for details
        
    Inherits from ̀luigi.Task`
    
    Attributes
    ----------
    timestamp : datetime.datetime
    city : str
    """
    timestamp = luigi.DateHourParameter(default=dt.now())
    city = luigi.Parameter()

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime("%HH%M") # 16H35
        path = os.path.join(DATADIR, self.city, '{year}', '{month:02d}', '{day:02d}',
                            'forecast', '{ts}.json')
        return luigi.LocalTarget(path.format(year=year, month=month, day=day, ts=ts),
                                 format=MixedUnicodeBytes)

    def run(self):
        resp = weather(self.city, 'forecast')
        # just try the json conversion
        _ = resp.json()
        with self.output().open('w') as fobj:
            fobj.write(resp.content)


class CurrentCityWeatherCSV(luigi.Task): 
    """Luigi task for getting weather information for a specific place in a
    *csv* file

    :warning: Need the definition of a API key stored in
    `jitenshea/config.ini`, see [OpenWeather
    website](https://openweathermap.org/api) for details
        
    Inherits from ̀luigi.Task`
            
    Attributes
    ----------
    timestamp : datetime.datetime
    city : str
    """
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=10)
    city = luigi.Parameter()

    def requires(self):
        return CurrentCityWeatherJson(self.timestamp, self.city)

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime("%HH%M") # 16H35
        path = os.path.join(DATADIR, self.city, '{year}', '{month:02d}', '{day:02d}',
                            'current', '{ts}.csv')
        return luigi.LocalTarget(path.format(year=year, month=month, day=day, ts=ts),
                                 format=UTF8)

    def run(self):
        with self.input().open('r') as fobj:
            data = json.load(fobj)
        weather_id = data['weather'][0]['id']
        weather_main = data['weather'][0]['main']
        wind_speed = data['wind']['speed'] # m/s unit
        humidity = data['main']['humidity'] # in %
        temp = data['main']['temp'] # C unit
        temp_min = data['main']['temp_min'] # C unit
        temp_max = data['main']['temp_max'] # C unit
        pressure = data['main']['pressure'] # hPa unit
        cloudiness = data['clouds']['all'] # in %
        values = [self.timestamp, weather_id, weather_main, temp,
                  temp_min, temp_max, pressure, humidity, wind_speed, cloudiness]
        columns = ["date", "weather_id", "weather_desc", "temp", "temp_min",
                   "temp_max", "pressure", "humidity", "wind_speed", "cloudiness"]
        df = pd.DataFrame([values], columns=columns)
        with self.output().open('w') as fobj:
            df.to_csv(fobj, index=False)


class ForecastCityWeatherCSV(luigi.Task):
    """Luigi task for getting weather forecasts at a specific date and place in
    a *csv* file

    :warning: Need the definition of a API key stored in
    `jitenshea/config.ini`, see [OpenWeather
    website](https://openweathermap.org/api) for details
        
    Inherits from ̀luigi.Task`
    
    Attributes
    ----------
    timestamp : datetime.datetime
    city : str
    """
    timestamp = luigi.DateHourParameter(default=dt.now())
    city = luigi.Parameter()

    def requires(self):
        return ForecastCityWeatherJson(self.timestamp, self.city)

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime("%HH%M") # 16H35
        path = os.path.join(DATADIR, self.city, '{year}', '{month:02d}', '{day:02d}',
                            'forecast', '{ts}.csv')
        return luigi.LocalTarget(path.format(year=year, month=month, day=day, ts=ts),
                                 format=UTF8)

    def run(self):
        with self.input().open('r') as fobj:
            data = json.load(fobj)['list']
        def get(single):
            """get data for a single forecast
            """
            rain = single.get('rain', {'3h': None})
            snow = single.get('snow', {'3h': None})
            return {"forecast_at": self.timestamp,
                    "weather_id": single['weather'][0]['id'],
                    "weather_desc": single['weather'][0]['main'],
                    "wind_speed": single['wind']['speed'], # m/s unit,
                    "humidity": single['main']['humidity'], # in %
                    "temp": single['main']['temp'], # C unit,
                    "temp_min": single['main']['temp_min'], # C unit
                    "temp_max": single['main']['temp_max'], # C unit
                    "pressure": single['main']['pressure'], # hPa unit
                    "rain_3h": rain.get('3h', None), # rain volume mm unit
                    "snow_3h": snow.get('3h', None), # snow volume
                    "cloudiness": single['clouds']['all'], # in %
                    "ts": pd.Timestamp.fromtimestamp(single['dt'])}
        columns = ["forecast_at", "ts", "weather_id", "weather_desc", "temp",
                   "temp_min", "temp_max", "rain_3h", "snow_3h", "pressure",
                   "humidity", "wind_speed", "cloudiness"]
        df = pd.DataFrame([get(x) for x in data]).sort_values(by="ts")
        with self.output().open('w') as fobj:
            df[columns].to_csv(fobj, index=False)
