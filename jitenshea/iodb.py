# coding: utf-8

"""Some function to read and write with a PostgreSQL/PostGIS database
"""

import os
import logging

from jitenshea import config


logger = logging.getLogger(__name__)


def psql_args():
    """Return the arguments for the command psql with some db parameters

    Return a list of str
    """
    psql = ['-h', config['database']['host'], '-d', config['database']['dbname'], '-U',
            config['database']['user'], '-p',  config['database']['port']]
    if 'password' in config['database'] and config['database']['password'] is not None:
        psql.insert(0, 'PGPASSWORD={pwd}'.format(pwd=config['database']['password']))
    return psql

def shp2pgsql_args(projection, filename, tablename, encoding=None):
    """Return the arguments for the command shp2pgsql

    projection: str
       Projection SRID no.
    filename: str
       Shapefile
    tablename: str
       Name of the SQL table
    encoding: str (default None)

    Return a list of str
    """
    logger.info("commands line for shp2pgsql with the file '%s'", filename)
    shp2pgsql = ['-cID']
    if encoding:
        shp2pgsql.extend(['-W', encoding])
    if projection:
        shp2pgsql.extend(['-s', projection])
    shp2pgsql.extend([filename, tablename])
    return shp2pgsql
