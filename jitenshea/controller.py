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
from jitenshea.stats import find_cluster
from jitenshea.iodb import db


daiquiri.setup(level=logging.INFO)
logger = daiquiri.getLogger(__name__)

CITIES = ('bordeaux',
          'lyon')
TimeWindow = namedtuple('TimeWindow', ['start', 'stop', 'order_reference_date'])


def processing_daily_data(rset, window):
    """Re arrange the daily transactions data when it's necessary

    Parameters
    ----------
    rset : sqlalchemy.engine.result.ResultProxy
        Result of a SQL query
    window : integer
        Time windows

    Returns
    -------
    list of dicts
        Rearranged list of transactions

    """
    if not rset:
        return {"data": []}
    data = [dict(zip(x.keys(), x)) for x in rset]
    if window == 0:
        return data
    # re-arrange the result set to get a list of values for the keys 'date' and 'value'
    values = []
    for k, group in groupby(data, lambda x: x['id']):
        group = list(group)
        values.append({'id': k,
                       "date": [x['date'] for x in group],
                       'value': [x['value'] for x in group],
                       'name': group[0]['name']})
    return {"data": values}


def processing_timeseries(rset):
    """Processing the result of a timeseries SQL query

    Parameters
    ----------
    rset : sqlalchemy.engine.result.ResultProxy
        Result of a SQL query

    Returns
    -------
    a list of dicts
        Bike and stand availability timeseries for given stations

    """
    if not rset:
        return []
    data = [dict(zip(x.keys(), x)) for x in rset]
    values = []
    for k, group in groupby(data, lambda x: x['id']):
        group = list(group)
        values.append({'id': k,
                       'name': group[0]['name'],
                       "ts": [x['timestamp'] for x in group],
                       'available_bikes': [x['available_bikes'] for x in group],
                       'available_stands': [x['available_stands'] for x in group]})
    return {"data": values}


def time_window(day, window, backward):
    """Give a start and stop according to the size of `window` and the `backward`
    parameter. The `order_reference_date` is used to fix the date values to sort
    station by values.

    Parameters
    ----------
    day : date
       Start or stop according to the backward parameter
    window : int
       Number of day before (resp. after) the 'day' parameter
    backward : boolean

    Returns
    -------
    TimeWindow
        Ttime between start and stop dates, ordered either by start or stop

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

    Parameters
    ----------
    stations : list of integer
        Stations IDs

    Returns
    -------
    dict
        Stations into the GeoJSON format
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


def clustered_station_geojson(stations):
    """Process station data into GeoJSON

    Parameters
    ----------
    stations : list of dicts
        Clustered stations

    Returns
    -------
    dict
        Clustered stations formatted as a GeoJSon object
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
                 "cluster_id": data['cluster_id'],
                 "name": data['name'],
                 "start": data['start'],
                 "stop": data['stop']
             }})
    return {"type": "FeatureCollection", "features": result}


def cities():
    """Manually build the list of considered cities

    TODO: recover the number of stations automatically by requesting the database
    # Lyon
    # select count(*) from lyon.pvostationvelov;
    # Bdx
    # select count(*) from bordeaux.vcub_station;

    Returns
    -------
    dict
        List of cities depicted into the API

    """
    return {"data": [{'city': 'lyon',
                      'country': 'france',
                      'stations': 348},
                     {'city': 'bordeaux',
                      'country': 'france',
                      'stations': 174}]}


def stations(city, limit, geojson):
    """Get a set of the `limit` first bicycle stations in `city`; if `geojson` is True, handle GeoJSON format

    Parameters
    ----------
    city : string
    limit : int
    geojson : boolean

    Returns
    -------
    a list of dict
        One dict by bicycle station
    """
    query = _query_stations(city, limit)
    eng = db()
    rset = eng.execute(query)
    keys = rset.keys()
    result = [dict(zip(keys, row)) for row in rset]
    if geojson:
        return station_geojson(result)
    return {"data": result}


def specific_stations(city, ids):
    """List of specific bicycle stations.

    Parameters
    ----------
    city : string
    ids : list

    Returns
    -------
    list of dict
        One dict by bicycle station
    """
    query = _query_stations(city, 1).replace("LIMIT 1", 'WHERE id IN %(id_list)s')
    eng = db()
    rset = eng.execute(query, id_list=tuple(str(x) for x in ids)).fetchall()
    if not rset:
        return []
    return {"data": [dict(zip(x.keys(), x)) for x in rset]}

def _query_stations(city, limit=20):
    """Query to get the list of bicycle stations

    Parameters
    ----------
    city : str
    limit : int

    Returns
    -------
    str
    """
    return """SELECT id
      ,name
      ,address
      ,city
      ,nb_stations as nb_bikes
      ,st_x(geom) as x
      ,st_y(geom) as y
    FROM {schema}.{table}
    LIMIT {limit}
    """.format(schema=city,
               table=config['database']['stations'],
               limit=limit)

def daily_query(city):
    """SQL query to get daily transactions according to `city`

    Parameters
    ----------
    city : str

    Returns
    -------
    str
        SQL request
    """
    if city not in ('bordeaux', 'lyon'):
        raise ValueError("City '{}' not supported.".format(city))
    return """SELECT id
           ,number AS value
           ,date
           ,name
        FROM {schema}.{table} AS X
        LEFT JOIN {schema}.{station} AS Y using(id)
        WHERE id IN %(id_list)s AND date >= %(start)s AND date <= %(stop)s
        ORDER BY id,date""".format(schema=city,
                                   table=config['database']['daily_transaction'],
                                   station=config['database']['stations'])


def daily_query_stations(city, limit, order_by='station'):
    """SQL query to get daily transactions for all stations

    Parameters
    ----------
    city : string
        City to consider, either 'bordeaux' or 'lyon'
    limit : integer
        Number of daily transactions to consider
    order_by : str
        Sorting attribute, default is 'station'

    Returns
    -------
    str
        SQL request
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
            FROM {schema}.{table}
            WHERE date = %(order_reference_date)s
            ORDER BY {order_by}
            LIMIT {limit}
            )
        SELECT S.id
          ,D.number AS value
          ,D.date
          ,Y.name
        FROM station AS S
        LEFT JOIN {schema}.{table} AS D ON (S.id=D.id)
        LEFT JOIN {schema}.{station} AS Y ON S.id=Y.id
        WHERE D.date >= %(start)s AND D.date <= %(stop)s
        ORDER BY S.rank,D.date;""".format(schema=config[city]['schema'],
                                          table=config['database']['daily_transaction'],
                                          station=config['database']['stations'],
                                          order_by=order_by,
                                          limit=limit)


def daily_transaction(city, station_ids, day, window=0, backward=True):
    """Retrieve the daily transactions for the stations `station_ids` in `city`

    Parameters
    ----------
    stations_ids : list of int
        Station IDs
    day : date
        Date around which transactions must be focused
    window : int
        Number of days to look around (default=0)
    backward : bool
        If True, get data before the date (default=True)

    Returns
    -------
    a list of dicts
        Daily transactions (number of transactions by station and by day) in `city`
    """
    window = time_window(day, window, backward)
    query = daily_query(city)
    eng = db()
    rset = eng.execute(query,
                       id_list=tuple(str(x) for x in station_ids),
                       start=window.start, stop=window.stop).fetchall()
    return processing_daily_data(rset, window)


def daily_transaction_list(city, day, limit, order_by, window=0, backward=True):
    """Retrieve daily transactions for the `limit` first stations in `city`

    Parameters
    ----------
    city : str
        City to consider, either 'bordeaux' or 'lyon'
    day : date
        Date around which transactions must be focused
    limit : int
        Number of transactions to consider
    order_by : str
        Transaction sorting attribute
    window : int
        Number of days to consider around `date` (default=0)
    backward: bool
        If True, get data before the date (default=True)

    Returns
    -------
    a list of dicts
        Daily transactions (number of transactions by station and by day) in `city`

    """
    window = time_window(day, window, backward)
    query = daily_query_stations(city, limit, order_by)
    eng = db()
    rset = eng.execute(query, start=window.start, stop=window.stop,
                       order_reference_date=window.order_reference_date).fetchall()
    return processing_daily_data(rset, window)


def timeseries(city, station_ids, start, stop):
    """Get timeseries data between dates `start` and `stop` for stations
    `station_ids` in `city`

    Parameters
    ----------
    city : str
        City to consider, either 'bordeaux' or 'lyon'
    station_ids : list of integer
        Bike stations to consider
    start : date
        Beginning of the period
    stop : date
        End of the period

    Returns
    -------
    list of dicts
        Bike and stand availability timeseries

    """
    query = """SELECT T.*
      ,S.name as name
    FROM {schema}.{table} AS T
    LEFT JOIN {schema}.{station} AS S using(id)
    WHERE id IN %(id_list)s AND timestamp >= %(start)s AND timestamp < %(stop)s
    ORDER BY id,timestamp
    """.format(schema=config[city]['schema'],
               table=config['database']['timeseries'],
               station=config['database']['stations'])
    eng = db()
    rset = eng.execute(query, id_list=tuple(x for x in station_ids),
                       start=start, stop=stop)
    return processing_timeseries(rset)


def hourly_process(df):
    """Timeseries into a hourly transaction profile

    Parameters
    ----------
    df : pandas.DataFrame
        Timeseries bike data for one specific station

    Returns
    -------
    pandas.DataFrame
        Transactions sum & mean for each hour

    """
    df = df.copy().set_index('ts')
    transaction = (df['available_bikes']
                   .diff()
                   .abs()
                   .dropna()
                   .resample('H')
                   .sum()
                   .reset_index())
    transaction['hour'] = transaction['ts'].apply(lambda x: x.hour)
    return transaction.groupby('hour')['available_bikes'].agg(['sum', 'mean'])


def hourly_profile(city, station_ids, day, window):
    """Compute the number of transactions per hour

    Note: for `window` parameter, quite annoying to convert np.int64,
    np.float64 from the DataFrame to JSON, even if you convert the DataFrame to
    dict. So, I use the .tolist() np.array method for the index and each
    column.

    Parameters
    ----------
    city : str
        City to consider, either 'bordeaux' or 'lyon'
    station_ids : list of integer
        Shared bike stations IDs
    day : date
        Day around which transactions must be scanned
    window : int
        Number of days to consider around `date`

    Returns
    -------
    a list of dicts
        Per-hour transactions, for given `city` and `station_ids` and dates

    """
    start = day - timedelta(window)
    result = []
    for data in timeseries(city, station_ids, start, day)["data"]:
        df = pd.DataFrame(data)
        profile = hourly_process(df)
        result.append({
            'id': data['id'],
            'name': data['name'],
            'hour': profile.index.values.tolist(),
            'sum': profile['sum'].values.tolist(),
            'mean': profile['mean'].values.tolist()})
    return {"data": result, "date": day, "window": window}


def daily_profile_process(df):
    """DataFrame with dates into a daily transaction profile

    Parameters
    ----------
    df : pandas.DataFrame - bike data timeseries for one specific station

    Returns
    -------
    pandas.DataFrame
        Transactions sum & mean for each day of the week

    """
    df = df.copy()
    df['weekday'] = df['date'].apply(lambda x: x.weekday())
    return df.groupby('weekday')['value'].agg(['sum', 'mean'])


def daily_profile(city, station_ids, day, window):
    """Compute the number of transaction per week day

    Note: for `window` parameter, it is quite annoying to convert np.int64,
    np.float64 from the DataFrame to JSON, even if you convert the DataFrame to
    dict. So, I use the .tolist() np.array method for the index and each
    column.

    Parameters
    ----------
    city : str
    stations_ids : list
    day : date
    window : int
        Number of days

    Returns
    -------
    a list of dicts
        Number of transaction per week day

    """
    result = []
    for data in daily_transaction(city, station_ids, day, window)["data"]:
        df = pd.DataFrame(data)
        profile = daily_profile_process(df)
        result.append({
            'id': data['id'],
            'name': data['name'],
            'day': profile.index.values.tolist(),
            'sum': profile['sum'].values.tolist(),
            'mean': profile['mean'].values.tolist()})
    return {"data": result, "date": day, "window": window}


def get_station_ids(city):
    """Provides the list of shared-bike station IDs

    Parameters
    ----------
    city : str
        City of interest, either `bordeaux` or `lyon`

    Returns
    -------
    list of integers
        IDs of the shared-bike stations in the `city`
    """
    query = ("SELECT id FROM {schema}.{table}"
             ";").format(schema=config[city]["schema"],
                         table=config['database']['stations'])
    eng = db()
    rset = eng.execute(query).fetchall()
    if not rset:
        return []
    return [row[0] for row in rset]


def station_cluster_query(city):
    """SQL query to get cluster IDs for some shared_bike stations within `city`

    Parameters
    ----------
    city : str
        City of interest, either ̀bordeaux` or `lyon`

    Returns
    -------
    str
        SQL query that gives the cluster ID for shared-bike stations in `city`
    """
    if city not in ('bordeaux', 'lyon'):
        raise ValueError("City '{}' not supported.".format(city))
    return ("WITH ranked_clusters AS ("
            "SELECT cs.station_id AS id, "
            "cs.cluster_id, "
            "cs.start AS start, "
            "cs.stop AS stop, "
            "citystation.name AS name, "
            "citystation.geom AS geom, "
            "rank() OVER (ORDER BY stop DESC) AS rank "
            "FROM {schema}.{cluster} AS cs "
            "JOIN {schema}.{station} AS citystation "
            "ON citystation.id = cs.station_id "
            "WHERE cs.station_id IN %(id_list)s) "
            "SELECT id, cluster_id, start, stop, name, "
            "st_x(geom) as x, "
            "st_y(geom) as y "
            "FROM ranked_clusters "
            "WHERE rank=1"
            ";").format(schema=config[city]['schema'],
                        cluster=config['database']['clustering'],
                        station=config['database']['stations'])


def station_clusters(city, station_ids=None, geojson=False):
    """Return the cluster IDs of shared-bike stations in `city`, when running a
    K-means algorithm between `day` and `day+window`

    Parameters
    ----------
    city : str
        City of interest, either `bordeaux` or `lyon`
    station_ids : list of integer
        Shared-bike station IDs ; if None, all the city stations are considered
    geojson : boolean
        If true, returns the clustered stations under the GeoJSON format

    Returns
    -------
    dict
        Cluster profiles for each cluster, at each hour of the day

    """
    if station_ids is None:
        station_ids = get_station_ids(city)
    query = station_cluster_query(city)
    eng = db()
    rset = eng.execute(query,
                       id_list=tuple(str(x) for x in station_ids))
    if not rset:
        logger.warning("rset is empty")
        return {"data": []}
    data = {"data": [dict(zip(rset.keys(), row)) for row in rset]}
    if geojson:
        return clustered_station_geojson(data["data"])
    return data


def cluster_profile_query(city):
    """SQL query to get cluster descriptions as 24-houred timeseries within `city`

    Parameters
    ----------
    city : str
        City of interest, either ̀bordeaux` or `lyon`

    Returns
    -------
    str
        SQL query that gives the timeseries cluster profile in `city`

    """
    if city not in ('bordeaux', 'lyon'):
        raise ValueError("City '{}' not supported.".format(city))
    return ("WITH ranked_centroids AS ("
            "SELECT *, rank() OVER (ORDER BY stop DESC) AS rank "
            "FROM {schema}.{centroid}) "
            "SELECT cluster_id, "
            "h00, h01, h02, h03, h04, h05, h06, h07, h08, h09, h10, h11, "
            "h12, h13, h14, h15, h16, h17, h18, h19, h20, h21, h22, h23, "
            "start, stop "
            "FROM ranked_centroids "
            "WHERE rank=1"
            ";").format(schema=config[city]['schema'],
                        centroid=config["database"]['centroids'])


def cluster_profiles(city):
    """Return the cluster profiles in `city`, when running a K-means algorithm
    between `day` and `day+window`

    Parameters
    ----------
    city : str
        City of interest, either `bordeaux` or `lyon`

    Returns
    -------
    dict
        Cluster profiles for each cluster, at each hour of the day
    """
    query = cluster_profile_query(city)
    eng = db()
    df = pd.io.sql.read_sql_query(query, eng)
    if df.empty:
        logger.warning("df is empty")
        return {"data": []}
    df = df.set_index('cluster_id')
    labels = find_cluster(df)
    result = []
    for cluster_id, cluster in df.iterrows():
        result.append({"cluster_id": cluster_id,
                       'label': labels[cluster_id],
                       "start": cluster['start'],
                       'stop': cluster['stop'],
                       'hour': list(range(24)),
                       'values': [cluster[h] for h in ["h{:02d}".format(i) for i in range(24)]]})
    return {"data": result}
