"""Luigi tasks to retrieve and process bike data

Highly inspired from the Tempus demo Luigi tasks that handle GrandLyon open
datasets: https://gitlab.com/Oslandia/tempus_demos

Supported cities:

* Bordeaux
  - stations URL:
  - real-time bike availability URL:

* Lyon
  - stations URL:
  - real-time bike availability URL:

"""

import os
import json
import zipfile
from datetime import datetime as dt
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import sh

import requests

import luigi
from luigi.contrib.postgres import CopyToTable, PostgresQuery
from luigi.format import UTF8, MixedUnicodeBytes

from jitenshea import config
from jitenshea.iodb import db, psql_args, shp2pgsql_args
from jitenshea.stats import compute_clusters, train_prediction_model

_HERE = os.path.abspath(os.path.dirname(__file__))
DATADIR = 'datarepo'

BORDEAUX_STATION_URL = 'https://data.bordeaux-metropole.fr/files.php?gid=43&format=2'
# BORDEAUX_STATION_URL = 'https://data.bordeaux-metropole.fr/wfs?service=wfs&request=GetFeature&version=2.0.0&key={key}&typename=CI_STVEL_P'
BORDEAUX_BIKEAVAILABILITY_URL = 'https://data.bordeaux-metropole.fr/wfs?service=wfs&request=GetFeature&version=2.0.0&key={key}&typename=CI_VCUB_P'

LYON_STATION_URL = 'https://download.data.grandlyon.com/wfs/grandlyon?service=wfs&request=GetFeature&version=2.0.0&SRSNAME=EPSG:4326&outputFormat=SHAPEZIP&typename=pvo_patrimoine_voirie.pvostationvelov'
LYON_BIKEAVAILABILITY_URL = 'https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json'


def yesterday():
    """Return the day before today
    """
    return date.today() - timedelta(1)


class CreateSchema(PostgresQuery):
    host = config['database']['host']
    database = config['database']['dbname']
    user = config['database']['user']
    password = config['database'].get('password')
    schema = luigi.Parameter()
    table = luigi.Parameter(default='create_schema')
    query = "CREATE SCHEMA IF NOT EXISTS {schema};"

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()
        sql = self.query.format(schema=self.schema)
        cursor.execute(sql)
        # Update marker table
        self.output().touch(connection)
        # commit and close connection
        connection.commit()
        connection.close()


class ShapefilesTask(luigi.Task):
    """Task to download a zip files which includes the shapefile

    Need the source: rdata or grandlyon and the layer name (i.e. typename).
    """
    city = luigi.Parameter()

    @property
    def path(self):
        return os.path.join(DATADIR, self.city,
                            '{}-stations.zip'.format(self.city))

    @property
    def url(self):
        if self.city == 'bordeaux':
            return BORDEAUX_STATION_URL
        elif self.city == 'lyon':
            return LYON_STATION_URL
        else:
            raise ValueError(("{} is an unknown city.".format(self.city)))

    def output(self):
        return luigi.LocalTarget(self.path, format=MixedUnicodeBytes)

    def run(self):
        with self.output().open('w') as fobj:
            resp = requests.get(self.url)
            resp.raise_for_status()
            fobj.write(resp.content)


class UnzipTask(luigi.Task):
    """Task dedicated to unzip file

    To get trace that the task has be done, the task creates a text file with
    the same same of the input zip file with the '.done' suffix. This generated
    file contains the path of the zipfile and all extracted files.
    """
    city = luigi.Parameter()

    @property
    def path(self):
        return os.path.join(DATADIR, self.city,
                            '{}-stations.zip'.format(self.city))

    def requires(self):
        return ShapefilesTask(self.city)

    def output(self):
        filepath = os.path.join(DATADIR, self.city, "unzip.done")
        return luigi.LocalTarget(filepath)

    def run(self):
        with self.output().open('w') as fobj:
            fobj.write("unzip {} stations at {}\n".format(self.city, dt.now()))
            zip_ref = zipfile.ZipFile(self.path)
            fobj.write("\n".join(elt.filename for elt in zip_ref.filelist))
            fobj.write("\n")
            zip_ref.extractall(os.path.dirname(self.input().path))
            zip_ref.close()


class ShapefileIntoDB(luigi.Task):
    """Dump a shapefile into a table
    """
    city = luigi.Parameter()
    table = "raw_stations"

    @property
    def projection(self):
        return config[self.city]['srid']

    @property
    def typename(self):
        return config[self.city]['typename']

    def requires(self):
        return {"zip": UnzipTask(city=self.city),
                "schema": CreateSchema(schema=self.city)}

    def output(self):
        filepath = '_'.join(['task', 'shp2pgsql', "to",
                             self.city, self.table, 'proj', self.projection])
        return luigi.LocalTarget(os.path.join(DATADIR, self.city,
                                              filepath + '.txt'))

    def run(self):
        table = self.city + '.' + self.table
        dirname = os.path.abspath(os.path.dirname(self.input()['zip'].path))
        shpfile = os.path.join(dirname, self.typename + '.shp')
        shp2args = shp2pgsql_args(self.projection, shpfile, table)
        psqlargs = psql_args()
        with self.output().open('w') as fobj:
            sh.psql(sh.shp2pgsql(shp2args), psqlargs)
            fobj.write("shp2pgsql {} at {}\n".format(shpfile, dt.now()))
            fobj.write("Create {schema}.{table}\n"
                       .format(schema=self.city, table=self.table))

class NormalizeStationTable(PostgresQuery):
    """
    """
    city = luigi.Parameter()

    host = config['database']['host']
    database = config['database']['dbname']
    user = config['database']['user']
    password = None

    query = ("DROP TABLE IF EXISTS {schema}.stations; "
             "CREATE TABLE {schema}.stations"
             " AS "
             "SELECT {id} AS id, {name} AS name, "
             "{address} AS address, {city} AS city, "
             "{nb_stations} AS nb_stations, geom "
             "FROM {schema}.raw_stations"
             ";")

    @property
    def table(self):
        return '{schema}.stations'.format(schema=self.city)

    def requires(self):
        return ShapefileIntoDB(self.city)

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()
        sql = self.query.format(schema=self.city,
                                id=config[self.city]['feature_id'],
                                name=config[self.city]['feature_name'],
                                address=config[self.city]['feature_address'],
                                city=config[self.city]['feature_city'],
                                nb_stations=config[self.city]['feature_nb_stations'])
        print(sql)
        cursor.execute(sql)
        # Update marker table
        self.output().touch(connection)
        # commit and close connection
        connection.commit()
        connection.close()
