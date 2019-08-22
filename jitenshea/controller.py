# coding: utf-8

"""Database controller for the Web Flask API
"""


import daiquiri

from itertools import groupby
from datetime import datetime, timedelta
from collections import namedtuple

import pandas as pd

from jitenshea.stats import find_cluster
from jitenshea.iodb import db


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
                       'nb_stands': group[0]['nb_stands'],
                       "ts": [x['timestamp'] for x in group],
                       'available_bikes': [x['available_bikes'] for x in group],
                       'available_stands': [x['available_stands'] for x in group]})
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


def station_geojson(stations, feature_list):
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
             "properties": {k: data[k] for k in feature_list}
            })
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

    Parameters
    ----------
    city : string
    limit : int
    geojson : boolean

    Returns
    -------
    list
        a list of dict, one dict by bicycle station
    """
    query = _query_stations(city, limit)
    eng = db()
    rset = eng.execute(query)
    keys = rset.keys()
    result = [dict(zip(keys, row)) for row in rset]
    if geojson:
        return station_geojson(result,
                               feature_list=['id', 'name', 'address', 'city', 'nb_stands'])
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
    query = """SELECT id
      ,name
      ,address
      ,city
      ,nb_stations as nb_stands
      ,st_x(geom) as x
      ,st_y(geom) as y
    FROM {schema}.station
    """.format(schema=city)
    if limit is not None:
        query += " LIMIT {limit}".format(limit=limit)
    return query

def daily_query(city):
    """SQL query to get daily transactions according to the city
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
                                   table='daily_transaction',
                                   station='station')


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
        ORDER BY S.rank,D.date;""".format(schema=city,
                                          table='daily_transaction',
                                          station='station',
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
    query = """SELECT T.*
      ,S.name as name
      ,S.nb_stations as nb_stands
    FROM {schema}.{table} AS T
    LEFT JOIN {schema}.{station} AS S using(id)
    WHERE id IN %(id_list)s AND timestamp >= %(start)s AND timestamp < %(stop)s
    ORDER BY id,timestamp
    """.format(schema=city,
               table='timeseries',
               station='station')
    eng = db()
    rset = eng.execute(query, id_list=tuple(x for x in station_ids),
                       start=start, stop=stop)
    return processing_timeseries(rset)


def prediction_timeseries(city, station_ids, start, stop,
                          values_num, with_current_values, freq='1H'):
    """Get bike availability predictions between `start` and `stop` dates for
    `city` at stations `station_ids`

    Parameters
    ----------
    city : str
        City of interest
    station_ids : list of ints
        Ids of the stations that are considered
    start : datetime
        Begin of prediction period
    stop : datetime
        End of prediction period
    values_num : int
        Number of predict values
    with_current_values : bool
        Include the current values?
    Returns
    -------
    list of dict
    """
    # get predicted data
    query = """SELECT T.station_id AS id
         , T.timestamp AS timestamp
         , T.nb_bikes AS nb_bikes
         , S.nb_stations as nb_stands
         , S.name AS name
         FROM {city}.prediction AS T
         LEFT JOIN {city}.station AS S ON (T.station_id::varchar = S.id::varchar)
         WHERE id IN %(id_list)s
           AND timestamp >= %(start)s AND timestamp < %(stop)s
           AND frequency = %(freq)s
         ORDER BY id,timestamp;""".format(city=city)
    eng = db()
    rset_pred = eng.execute(query, id_list=tuple(x for x in station_ids),
                            start=start, stop=stop, freq=freq)
    # current values
    rset_current = None
    if with_current_values:
        query = """select distinct id
           , timestamp
           , available_bikes as nb_bikes
           , S.nb_stations as nb_stands
           , S.name
        from {city}.timeseries as T
        left join {city}.station as S using(id)
        where id in %(id_list)s
           AND timestamp >= %(start)s and timestamp < %(stop)s
        """.format(city=city)
        rset_current = eng.execute(query, id_list=tuple(x for x in station_ids),
                                   start=start, stop=stop)
    pred = [dict(zip(x.keys(), x)) for x in rset_pred]
    # truncate the nth latest values and add the prediction time
    for data in pred[-values_num:]:
        data['at'] = freq
    current = []
    if rset_current:
        current = [dict(zip(x.keys(), x)) for x in rset_current]
        for data in current:
            data['at'] = '0'
    return current + pred[-values_num:]


def latest_availability(city, limit, geojson):
    """Get bike the latest bikes availability for a specific city.

    Parameters
    ----------
    city : str
        Name of the city
    limit : int
        Max number of stations
    geosjon : bool
        Data in geojson?
    freq : str
        Time horizon

    Returns
    -------
    dict
    """
    query = """with latest as (
      select id
        ,timestamp
        ,available_bikes as nb_bikes
        ,rank() over (partition by id order by timestamp desc) as rank
      from {city}.timeseries
      where timestamp >= %(min_date)s
    )
    select P.id
      ,P.timestamp
      ,P.nb_bikes
      ,S.name
      ,S.nb_stations as nb_stands
      ,st_x(S.geom) as x
      ,st_y(S.geom) as y
    from latest as P
    join {city}.station as S using(id)
    where P.rank=1
    order by id
    """.format(city=city)
    if limit is not None:
        query += " limit {limit}".format(limit=limit)
    eng = db()
    # avoid getting the full history
    min_date = datetime.now() - timedelta(days=2)
    rset = eng.execute(query, min_date=min_date)
    keys = rset.keys()
    result = [dict(zip(keys, row)) for row in rset]
    latest_date = max(x['timestamp'] for x in result)
    if geojson:
        return station_geojson(result, feature_list=['id', 'name', 'timestamp', 'nb_bikes', 'nb_stands'])
    return {"data": result, "date": latest_date}


def latest_predictions(city, limit, geojson, freq='1H'):
    """Get bike availability predictions for a specific city.

    Parameters
    ----------
    city : str
        Name of the city
    limit : int
        Max number of stations
    geosjon : bool
        Data in geojson?
    freq : str
        Time horizon

    Returns
    -------
    dict
    """
    query = """with latest as (
      select station_id as id
        ,timestamp
        ,nb_bikes
        ,rank() over (partition by station_id order by timestamp desc) as rank
      from {city}.prediction
      where frequency=%(freq)s
         and timestamp >= %(min_date)s
    )
    select P.id
      ,P.timestamp
      ,P.nb_bikes
      ,S.name
      ,S.nb_stations as nb_stands
      ,st_x(S.geom) as x
      ,st_y(S.geom) as y
    from latest as P
    join {city}.station as S using(id)
    where P.rank=1
    order by id
    """.format(city=city)
    if limit is not None:
        query += " limit {limit}".format(limit=limit)
    eng = db()
    # avoid getting the full history
    min_date = datetime.now() - timedelta(days=2)
    rset = eng.execute(query, freq=freq, min_date=min_date, limit=limit)
    keys = rset.keys()
    result = [dict(zip(keys, row)) for row in rset]
    predict_date = max(x['timestamp'] for x in result)
    if geojson:
        return station_geojson(result, feature_list=['id', 'name', 'timestamp', 'nb_bikes', 'nb_stands'])
    return {"data": result, "date": predict_date}


def hourly_process(df):
    """DataFrame with timeseries into a hourly transaction profile

    df: DataFrame
        timeseries bike data for one specific station

    Return a DataFrame with the transactions sum & mean for each hour
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
             ";").format(schema=city,
                         table='station')
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
            ";").format(schema=city,
                        cluster='clustering',
                        station='station')


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
            ";").format(schema=city,
                        centroid='centroid')


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
