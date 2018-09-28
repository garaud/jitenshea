"""Execute some SQL queries to retrieve data from the database
"""

import pandas as pd

from jitenshea.iodb import db


def latest_station_timewindow(city, start, stop):
    """Get latest available stations (by date) between `start` and `stop`.

    Parameters
    ----------
    city : str
        The name of the city.
    start : datetime
    stop : datetime

    Returns
    -------
    pd.DataFrame
    """
    query = """with ranked as (
    select distinct id as station_id
      , timestamp as ts
      , available_bikes as nb_bikes
      , available_stands as nb_stands
      , available_bikes::float / (available_bikes::float + available_stands::float) as probability
      -- rank over station_id and timestamp to take the most recent ones
      , rank() over (partition by id order by timestamp desc) as rk
    from {schema}.timeseries
    where timestamp >= %(start)s
    and timestamp < %(stop)s
    and (available_bikes > 0 or available_stands > 0)
    and status = 'open'
    order by id, timestamp
      )

    select station_id
      ,ts
      ,nb_bikes
      ,nb_stands
      ,probability
    from ranked
    where rk=1
    order by station_id;
    """.format(schema=city)
    eng = db()
    return pd.io.sql.read_sql_query(query, eng,
                                    params={"start": start,
                                            "stop": stop})
