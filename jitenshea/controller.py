# coding: utf-8

"""Database controller for the Web Flask API
"""


import daiquiri
import logging

from itertools import groupby
from datetime import timedelta
from collections import namedtuple

import pandas as pd

from jitenshea import config
from jitenshea.iodb import db


daiquiri.setup(level=logging.INFO)
logger = daiquiri.getLogger(__name__)

CITIES = ('bordeaux',
          'lyon')
TimeWindow = namedtuple('TimeWindow', ['start', 'stop', 'order_reference_date'])


def processing_daily_data(rset, window):
    """Re arrange when it's necessary the daily transactions data

    rset: ResultProxy by SQLAlchemy
        Result of a SQL query

    Return a list of dicts
    """
    if not rset:
        return []
    data = [dict(zip(x.keys(), x)) for x in rset]
    if window == 0:
        return data
    # re-arrange the result set to get a list of values for the keys 'date' and 'value'
    values = []
    for k, group in groupby(data, lambda x: x['id']):
        group = list(group)
        values.append({'id': k,
                       "date": [x['date'] for x in group],
                       'value': [x['value'] for x in group]})
    return {"data": values}

def processing_timeseries(rset):
    """Processing the result of a timeseries SQL query

    Return a list of dicts
    """
    if not rset:
        return []
    data = [dict(zip(x.keys(), x)) for x in rset]
    values = []
    for k, group in groupby(data, lambda x: x['id']):
        group = list(group)
        values.append({'id': k,
                       'name': group[0]['name'],
                       "ts": [x['ts'] for x in group],
                       'available_bike': [x['available_bike'] for x in group],
                       'available_stand': [x['available_stand'] for x in group]})
    return {"data": values}


def time_window(day, window, backward):
    """Return a TimeWindow

    Give a start and stop according to the size of the window and the backward
    parameter. The order_reference_date is used to fix the values date to sort
    station by values.

    day: date
       Start or stop according to the backward parameter
    window: int
       Number of day before (resp. after) the 'day' parameter
    backward: boolean

    Return TimeWindow
    """
    stop = day
    sign = 1 if backward else -1
    start = stop - timedelta(sign * window)
    order_reference_date = stop
    if not backward:
        start, stop = stop, start
        order_reference_date = start
    return TimeWindow(start, stop, order_reference_date)

def station_geojson(stations):
    """Process station data into GeoJSON
    """
    result = []
    for data in stations:
        result.append(
            {"type": "Feature",
             "geometry": {
                 "type": "Point",
                 "coordinates": [data['x'], data['y']]
             },
             "properties": {
                 "id": data['id'],
                 "name": data['name'],
                 "address": data['address'],
                 "city": data['city'],
                 "nb_bikes": data['nb_bikes']
             }})
    return {"type": "FeatureCollection", "features": result}

def cities():
    "List of cities"
    # Lyon
    # select count(*) from lyon.pvostationvelov;
    # Bdx
    # select count(*) from bordeaux.vcub_station;
    return {"data": [{'city': 'lyon',
                      'country': 'france',
                      'stations': 348},
                     {'city': 'bordeaux',
                      'country': 'france',
                      'stations': 174}]}

def stations(city, limit, geojson):
    """List of bicycle stations

    city: string
    limit: int
    geojson: boolean

    Return a list of dict, one dict by bicycle station
    """
    if city == 'bordeaux':
        query = bordeaux_stations(limit)
    elif city == 'lyon':
        query = lyon_stations(limit)
    else:
        raise ValueError("City {} not supported".format(city))
    eng = db()
    rset = eng.execute(query)
    keys = rset.keys()
    result = [dict(zip(keys, row)) for row in rset]
    if geojson:
        return station_geojson(result)
    return {"data": result}


def bordeaux_stations(limit=20):
    """Query for the list of bicycle stations in Bordeaux

    limit: int
       default 20

    Return a SQL query to execute
    """
    return """SELECT numstat::int AS id
      ,nom AS name
      ,adresse AS address
      ,commune AS city
      ,nbsuppor::int AS nb_bikes
      ,st_x(st_transform(geom, 4326)) AS x
      ,st_y(st_transform(geom, 4326)) AS y
    FROM {schema}.vcub_station
    LIMIT {limit}
    """.format(schema=config['bordeaux']['schema'],
               limit=limit)

def lyon_stations(limit=20):
    """Query for the list of bicycle stations in Lyon

    limit: int
       default 20

    Return a SQL query to execute
    """
    return """SELECT idstation::int AS id
      ,nom AS name
      ,adresse1 AS address
      ,commune AS city
      ,nbbornette::int AS nb_bikes
      ,st_x(geom) AS x
      ,st_y(geom) AS y
    FROM {schema}.pvostationvelov
    LIMIT {limit}
    """.format(schema=config['lyon']['schema'],
               limit=limit)

def bordeaux(station_ids):
    """Get some specific bicycle-sharing stations for Bordeaux
    station_id: list of int
       Ids of the bicycle-sharing station

    Return bicycle stations in a list of dict
    """
    query = bordeaux_stations(1).replace("LIMIT 1", 'WHERE numstat IN %(id_list)s')
    eng = db()
    rset = eng.execute(query, id_list=tuple(str(x) for x in station_ids)).fetchall()
    if not rset:
        return []
    return {"data" : [dict(zip(x.keys(), x)) for x in rset]}

def lyon(station_ids):
    """Get some specific bicycle-sharing stations for Lyon
    station_id: list of ints
       Ids of the bicycle-sharing stations

    Return bicycle stations in a list of dict
    """
    query = lyon_stations(1).replace("LIMIT 1", 'WHERE idstation IN %(id_list)s')
    eng = db()
    rset = eng.execute(query, id_list=tuple(str(x) for x in station_ids)).fetchall()
    if not rset:
        return []
    return {"data" : [dict(zip(x.keys(), x)) for x in rset]}


def daily_query(city):
    """SQL query to get daily transactions according to the city
    """
    if city not in ('bordeaux', 'lyon'):
        raise ValueError("City '{}' not supported.".format(city))
    return """SELECT id
           ,number AS value
           ,date
        FROM {schema}.daily_transaction
        WHERE id IN %(id_list)s AND date >= %(start)s AND date <= %(stop)s
        ORDER BY id,date""".format(schema=config[city]['schema'])

def daily_query_stations(city, limit, order_by='station'):
    """SQL query to get daily transactions for all stations
    """
    if city not in ('bordeaux', 'lyon'):
        raise ValueError("City '{}' not supported.".format(city))
    if order_by == 'station':
        order_by = 'id'
    if order_by == 'value':
        order_by = 'number DESC'
    return """WITH station AS (
            SELECT id
              ,row_number() over (partition by null order by {order_by}) AS rank
            FROM {schema}.daily_transaction
            WHERE date = %(order_reference_date)s
            ORDER BY {order_by}
            LIMIT {limit}
            )
        SELECT S.id
          ,D.number AS value
          ,D.date
        FROM station AS S
        LEFT JOIN {schema}.daily_transaction AS D ON (S.id=D.id)
        WHERE D.date >= %(start)s AND D.date <= %(stop)s
        ORDER BY S.rank,D.date;""".format(schema=config[city]['schema'],
                                          order_by=order_by,
                                          limit=limit)


def daily_transaction(city, station_ids, day, window=0, backward=True):
    """Retrieve the daily transaction for the Bordeaux stations

    stations_ids: list of int
        List of ids station
    day: date
        Data for this specific date
    window: int (0 by default)
        Number of days to look around the specific date
    backward: bool (True by default)
        Get data before the date or not, according to the window number

    Return a list of dicts
    """
    window = time_window(day, window, backward)
    query = daily_query(city)
    eng = db()
    rset = eng.execute(query,
                       id_list=tuple(str(x) for x in station_ids),
                       start=window.start, stop=window.stop).fetchall()
    return processing_daily_data(rset, window)


def daily_transaction_list(city, day, limit, order_by, window=0, backward=True):
    """Retrieve the daily transaction for the Bordeaux stations

    city: str
    day: date
        Data for this specific date
    limit: int
    order_by: str
    window: int (0 by default)
        Number of days to look around the specific date
    backward: bool (True by default)
        Get data before the date or not, according to the window number

    Return a list of dicts
    """
    window = time_window(day, window, backward)
    query = daily_query_stations(city, limit, order_by)
    eng = db()
    rset = eng.execute(query, start=window.start, stop=window.stop,
                       order_reference_date=window.order_reference_date).fetchall()
    return processing_daily_data(rset, window)

def timeseries(city, station_ids, start, stop):
    """Get timeseries data between two dates for a specific city and a list of station ids
    """
    query = """SELECT *
    FROM {schema}.timeserie_norm
    WHERE id IN %(id_list)s AND ts >= %(start)s AND ts < %(stop)s
    """.format(schema=config[city]['schema'])
    eng = db()
    rset = eng.execute(query, id_list=tuple(x for x in station_ids),
                       start=start, stop=stop)
    return processing_timeseries(rset)

def hourly_process(df):
    """DataFrame with timeseries into a hourly transaction profile

    df: DataFrame
        timeseries bike data for one specific station

    Return a DataFrame with the transactions sum & mean for each hour
    """
    df = df.copy().set_index('ts')
    transaction = (df['available_bike']
                   .diff()
                   .abs()
                   .dropna()
                   .resample('H')
                   .sum()
                   .reset_index())
    transaction['hour'] = transaction['ts'].apply(lambda x: x.hour)
    return transaction.groupby('hour')['available_bike'].agg(['sum', 'mean'])

def hourly_profile(city, station_ids, day, window):
    """Return the number of transaction per hour

    city: str
    station_ids: list
    day: date
    window: int
        number of days

    Note: quite annoying to convert np.int64, np.float64 from the DataFrame to
    JSON, even if you convert the DataFrame to dict. So, I use the .tolist()
    np.array method for the index and each column.

    Return a list of dicts
    """
    start = day - timedelta(window)
    result = []
    for data in timeseries(city, station_ids, start, day):
        df = pd.DataFrame(data)
        profile = hourly_process(df)
        result.append({
            'id': data['id'],
            'name': data['name'],
            'hour': profile.index.values.tolist(),
            'sum': profile['sum'].values.tolist(),
            'mean': profile['mean'].values.tolist()})
    return {"data": result}


def daily_profile_process(df):
    """DataFrame with dates into a daily transaction profile

    df: DataFrame
        timeseries bike data for one specific station

    Return a DataFrame with the transactions sum & mean for each day of the week
    """
    df = df.copy()
    df['weekday'] = df['date'].apply(lambda x: x.weekday())
    return df.groupby('weekday')['value'].agg(['sum', 'mean'])

def daily_profile(city, station_ids, day, window):
    """Return the number of transaction per day of week

    city: str
    stations_ids: list
    day: date
    window: int
        number of days

    Note: quite annoying to convert np.int64, np.float64 from the DataFrame to
    JSON, even if you convert the DataFrame to dict. So, I use the .tolist()
    np.array method for the index and each column.

    Return a list of dicts
    """
    result = []
    for data in daily_transaction(city, station_ids, day, window):
        df = pd.DataFrame(data)
        profile = daily_profile_process(df)
        result.append({
            'id': data['id'],
            'day': profile.index.values.tolist(),
            'sum': profile['sum'].values.tolist(),
            'mean': profile['mean'].values.tolist()})
    return {"data": result}
