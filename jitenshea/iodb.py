# coding: utf-8

"""Some function to read and write with a PostgreSQL/PostGIS database
"""

import os
import logging

from sqlalchemy import create_engine

from jitenshea import config


logger = logging.getLogger(__name__)


def psql_args():
    """Return the arguments for the command psql with some db parameters: host,
    database, user, (pwd,) port

    Return
    ------
    a list of str
    """
    psql = ['-h', config['database']['host'], '-d', config['database']['dbname'], '-U',
            config['database']['user'], '-p',  config['database']['port']]
    if 'password' in config['database'] and config['database']['password'] is not None:
        psql.insert(0, 'PGPASSWORD={pwd}'.format(pwd=config['database']['password']))
    return psql

def shp2pgsql_args(projection, filename, tablename, encoding=None):
    """Return the arguments for the command shp2pgsql

    Parameters
    ----------
    projection : str
        Projection SRID no.
    filename : str
        Shapefile
    tablename : str
        Name of the SQL table
    encoding : str
        (default None)

    Return
    ------
    a list of str
    """
    logger.info("commands line for shp2pgsql with the file '%s'", filename)
    shp2pgsql = ['-cID']
    if encoding:
        shp2pgsql.extend(['-W', encoding])
    if projection:
        shp2pgsql.extend(['-s', projection])
    shp2pgsql.extend([filename, tablename])
    return shp2pgsql

def db():
    """Create a SQLAlchemy engine with Postgres connection parameters contained
    in config.ini file

    Return
    ------
    SQLAlchemy engine
    """
    database = config['database']
    if database.get('password') is not None:
        url = 'postgresql://{user}:{password}@{host}/{dbname}'
        return create_engine(url.format(user=database['user'],
                                        password=database['password'],
                                        host=database['host'],
                                        dbname=database['dbname']))
    else:
        url = 'postgresql://{user}@{host}/{dbname}'
        return create_engine(url.format(user=database['user'],
                                        host=database['host'],
                                        dbname=database['dbname']))
