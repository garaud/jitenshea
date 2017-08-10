# coding: utf-8

"""Database controller for the Web Flask API
"""


import daiquiri
import logging

from jitenshea import config
from jitenshea.iodb import db


daiquiri.setup(level=logging.INFO)
logger = daiquiri.getLogger(__name__)

CITIES = ('bordeaux',
          'lyon')


def cities():
    "List of cities"
    # Lyon
    # select count(*) from lyon.pvostationvelov;
    # Bdx
    # select count(*) from bordeaux.vcub_station;
    return [{'city': 'lyon',
             'country': 'france',
             'stations': 348},
            {'city': 'bordeaux',
             'country': 'france',
             'stations': 174}]

def stations(city, limit):
    """List of bicycle stations

    city: string
    limit: int

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
    return [dict(zip(keys, row)) for row in rset]

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
        return {}
    return [dict(zip(x.keys(), x)) for x in rset]

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
    return [dict(zip(x.keys(), x)) for x in rset]
