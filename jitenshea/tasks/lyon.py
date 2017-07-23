# coding: utf-8

"""Luigi tasks to retrieve and process data for Lyon

Higly inspired from the Luigi tasks of the Tempus demo with the GrandLyon open datasets.
https://gitlab.com/Oslandia/tempus_demos
but dedicated to the bicycle-sharing data
"""

import os
import json
import zipfile
from datetime import datetime as dt
from datetime import date, timedelta

import sh

import requests

import luigi
import luigi.postgres
from luigi.format import UTF8, MixedUnicodeBytes

import pandas as pd

from jitenshea import config
from jitenshea.iodb import db, psql_args, shp2pgsql_args


_HERE = os.path.abspath(os.path.dirname(__file__))
WFS_RDATA_URL = "https://download.data.grandlyon.com/wfs/rdata"
WFS_GRANDLYON_URL = "https://download.data.grandlyon.com/wfs/grandlyon"
DEFAULT_PARAMS = {'SERVICE': 'WFS',
                  'VERSION': '2.0.0',
                  'request': 'GetFeature'}
DATADIR = 'datarepo/lyon'


def params_factory(projection, output_format, dataname):
    """return a new dict for HTTP query params

    Used for the wfs http query to get some data.
    """
    res = {"SRSNAME": 'EPSG:' + projection,
           "outputFormat": output_format,
           "typename": dataname}
    res.update(DEFAULT_PARAMS)
    return res

def yesterday():
    """Return the day before today
    """
    return date.today() - timedelta(1)


class ShapefilesTask(luigi.Task):
    """Task to download a zip files which includes the shapefile

    Need the source: rdata or grandlyon and the layer name (i.e. typename).
    """
    source = luigi.Parameter()
    typename = luigi.Parameter()
    path = os.path.join(DATADIR , '{typename}.zip')
    srid = 4326

    def output(self):
        return luigi.LocalTarget(self.path.format(typename=self.typename),
                                 format=MixedUnicodeBytes)

    def run(self):
        if self.source == 'rdata':
            url = WFS_RDATA_URL
        elif self.source == 'grandlyon':
            url = WFS_GRANDLYON_URL
        else:
            raise Exception("source {} not supported".format(self.source))
        params = params_factory(str(self.srid), 'SHAPEZIP', self.typename)
        with self.output().open('w') as fobj:
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            fobj.write(resp.content)


class UnzipTask(luigi.Task):
    """Task dedicated to unzip file

    To get trace that the task has be done, the task creates a text file with
    the same same of the input zip file with the '.done' suffix. This generated
    file contains the path of the zipfile and all extracted files.
    """
    source = luigi.Parameter(default='grandlyon')
    typename = luigi.Parameter()
    path = os.path.join(DATADIR , '{typename}.zip')

    def requires(self):
        return ShapefilesTask(self.source, self.typename)

    def output(self):
        filepath = os.path.join(DATADIR, "unzip-" + self.typename + '.done')
        return luigi.LocalTarget(filepath)

    def run(self):
        dirname = os.path.dirname(self.input().path)
        with self.output().open('w') as fobj:
            fobj.write("unzip {} at {}\n".format(self.typename, dt.now()))
            zip_ref = zipfile.ZipFile(os.path.join(dirname, self.typename + ".zip"), 'r')
            fobj.write("\n".join(elt.filename for elt in zip_ref.filelist))
            fobj.write("\n")
            zip_ref.extractall(dirname)
            zip_ref.close()


class CreateSchema(luigi.postgres.PostgresQuery):
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
        # print(sql)
        # print(extract_tablename(self.table))
        # logger.info('Executing query from task: {name}'.format(name=self.__class__))
        cursor.execute(sql)
        # Update marker table
        self.output().touch(connection)
        # commit and close connection
        connection.commit()
        connection.close()


class ShapefileIntoDB(luigi.Task):
    """Dump a shapefile into a table
    """
    source = luigi.Parameter(default="grandlyon")
    typename = luigi.Parameter()
    # table = luigi.Parameter()
    schema = luigi.Parameter(default=config['lyon']["schema"])
    projection = luigi.Parameter(default='4326')

    @property
    def table(self):
        if '.' in self.typename:
            return self.typename.split('.')[-1]
        return self.typename

    def requires(self):
        return {"zip": UnzipTask(source=self.source, typename=self.typename),
                "schema": CreateSchema(schema=self.schema)}

    def output(self):
        filepath = '_'.join(['task', 'shp2pgsql', self.typename, "to",
                             self.schema, self.table, 'proj', self.projection])
        return luigi.LocalTarget(os.path.join(DATADIR, filepath + '.txt'))

    def run(self):
        table = self.schema + '.' + self.table
        dirname = os.path.abspath(os.path.dirname(self.input()['zip'].path))
        shpfile = os.path.join(dirname, self.typename + '.shp')
        shp2args = shp2pgsql_args(self.projection, shpfile, table)
        psqlargs = psql_args()
        # check if the schema exist. raise if this is not the case
        with self.output().open('w') as fobj:
            sh.psql(sh.shp2pgsql(shp2args), psqlargs)
            fobj.write("shp2pgsql {} at {}\n".format(shpfile, dt.now()))
            fobj.write("Create {schema}.{table}\n"
                       .format(schema=self.schema, table=self.table))


class VelovStationAvailability(luigi.Task):
    """Get in real-time the shared cycle stations avaibility in a JSON format.

    Get data every 5 minutes
    """
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)
    path = os.path.join(DATADIR, '{year}', '{month:02d}', '{day:02d}', '{ts}.json')

    def requires(self):
        return ShapefileIntoDB(typename='pvo_patrimoine_voirie.pvostationvelov')

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime("%HH%M") # 16H35
        return luigi.LocalTarget(self.path.format(year=year, month=month, day=day, ts=ts), format=UTF8)

    def run(self):
        url = 'https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json'
        with self.output().open('w') as fobj:
            resp = requests.get(url)
            resp.raise_for_status
            data = resp.json()
            json.dump(resp.json(), fobj, ensure_ascii=False)


class VelovStationJSONtoCSV(luigi.Task):
    """Turn real-time velov station data JSON file to a CSV.
    """
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)
    path = os.path.join(DATADIR, '{year}', '{month:02d}', '{day:02d}', '{ts}.csv')
    keepcols = ['number', 'last_update', 'bike_stands', 'available_bike_stands',
                'available_bikes', 'availabilitycode', 'availability', 'bonus',
                'status']

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime("%HH%M") # 16H35
        return luigi.LocalTarget(self.path.format(year=year, month=month, day=day, ts=ts), format=UTF8)

    def requires(self):
        return VelovStationAvailability(self.timestamp)

    def run(self):
        with self.input().open() as fobj:
            data = json.load(fobj)
            df = pd.DataFrame(data['values'], columns=data['fields'])
        with self.output().open('w') as fobj:
            df[self.keepcols].to_csv(fobj, index=False)


class VelovStationDatabase(luigi.postgres.CopyToTable):
    """Insert Velov stations data into a PostgreSQL table
    """
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)

    host = 'localhost'
    database = config['database']['dbname']
    user = config['database']['user']
    password = None
    table = '{schema}.{tablename}'.format(schema=config['lyon']['schema'],
                                          tablename=config['lyon']['table'])

    columns = [('number', 'INT'),
               ('last_update', 'TIMESTAMP'),
               ('bike_stands', 'INT'),
               ('available_bike_stands', 'INT'),
               ('available_bikes', 'INT'),
               ('availabilitycode', 'INT'),
               ('availability', 'VARCHAR(20)'),
               ('bonus', 'VARCHAR(12)'),
               ('status', 'VARCHAR(12)')]

    def rows(self):
        """overload the rows method to skip the first line (header)
        """
        with self.input().open('r') as fobj:
            df = pd.read_csv(fobj)
            for idx, row in df.iterrows():
                yield row.values

    def requires(self):
        return VelovStationJSONtoCSV(self.timestamp)


class AggregateTransaction(luigi.Task):
    """Aggregate bicycle-share transactions data into a CSV file.
    """
    date = luigi.DateParameter(default=yesterday())
    path = os.path.join(DATADIR, '{year}', '{month:02d}', '{day:02d}', 'transactions.csv')

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.date)
        return luigi.LocalTarget(self.path.format(year=year, month=month, day=day), format=UTF8)

    def run(self):
        query = """SELECT DISTINCT * FROM {schema}.{tablename}
          WHERE last_update >= %(start)s AND last_update < %(stop)s
          ORDER BY last_update,number""".format(schema=config["lyon"]["schema"],
                                                tablename=config['lyon']['table'])
        eng = db()
        df = pd.io.sql.read_sql_query(query, eng, params={"start": self.date,
                                                          "stop": self.date + timedelta(1)})
        transactions = (df.query("status == 'OPEN'")
                        .groupby("number")['available_bikes']
                        .apply(lambda s: s.diff().abs().sum())
                        .dropna()
                        .to_frame()
                        .reset_index())
        transactions = transactions.rename_axis({"available_bikes": "transactions"}, axis=1)
        with self.output().open('w') as fobj:
            transactions.to_csv(fobj, index=False)


class AggregateLyonTransactionIntoDB(luigi.postgres.CopyToTable):
    """Aggregate bicycle-share transactions data into the database.
    """
    date = luigi.DateParameter(default=yesterday())

    host = 'localhost'
    database = config['database']['dbname']
    user = config['database']['user']
    password = None
    table = '{schema}.{tablename}'.format(schema=config['lyon']['schema'],
                                          tablename=config['lyon']['daily_transaction'])
    columns = [('id', 'INT'),
               ('number', 'FLOAT'),
               ('date', 'DATE')]

    def rows(self):
        """overload the rows method to skip the first line (header) and add date value
        """
        with self.input().open('r') as fobj:
            next(fobj)
            for line in fobj:
                yield line.strip('\n').split(',') + [self.date]

    def requires(self):
        return AggregateTransaction(self.date)
