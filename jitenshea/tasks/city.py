"""Luigi tasks to retrieve and process bike data

Supported cities:

* Bordeaux
  - stations URL: https://data.bordeaux-metropole.fr/files.php?gid=43&format=2
  - real-time bike availability URL: https://data.bordeaux-metropole.fr/wfs?service=wfs&request=GetFeature&version=2.0.0&typename=CI_VCUB_P

* Lyon
  - stations URL: https://download.data.grandlyon.com/wfs/grandlyon?service=wfs&request=GetFeature&version=2.0.0&SRSNAME=EPSG:4326&outputFormat=SHAPEZIP&typename=pvo_patrimoine_voirie.pvostationvelov
  - real-time bike availability URL: https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json
"""

import os
import json
import zipfile
from datetime import datetime as dt
from datetime import date, timedelta

from lxml import etree

import pandas as pd

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
BORDEAUX_BIKEAVAILABILITY_URL = 'https://data.bordeaux-metropole.fr/wfs?service=wfs&request=GetFeature&version=2.0.0&key={key}&typename=CI_VCUB_P'

LYON_STATION_URL = 'https://download.data.grandlyon.com/wfs/grandlyon?service=wfs&request=GetFeature&version=2.0.0&SRSNAME=EPSG:4326&outputFormat=SHAPEZIP&typename=pvo_patrimoine_voirie.pvostationvelov'
LYON_BIKEAVAILABILITY_URL = 'https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json'


def yesterday():
    """Return the day before today
    """
    return date.today() - timedelta(1)


def extract_xml_feature(node, namespace='{http://data.bordeaux-metropole.fr/wfs}'):
    """Return some attributes from XML/GML file for one specific station
    """
    get = lambda x: node.findtext(namespace + x)
    return [("gid", int(get("GID"))),
            ("ident", int(get("IDENT"))),
            ("type", get("TYPE")),
            ("nom", get("NOM")),
            ("etat", get("ETAT")),
            ("nbplaces", int(get('NBPLACES'))),
            ("nbvelos", int(get("NBVELOS"))),
            ("heure", pd.Timestamp(get("HEURE")))]


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
        cursor.execute(sql)
        # Update marker table
        self.output().touch(connection)
        # commit and close connection
        connection.commit()
        connection.close()


class BikeAvailability(luigi.Task):
    """
    """
    city = luigi.Parameter()
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)

    @property
    def path(self):
        if self.city == 'bordeaux':
            return os.path.join(DATADIR, self.city, '{year}',
                                '{month:02d}', '{day:02d}', '{ts}.xml')
        elif self.city == 'lyon':
            return os.path.join(DATADIR, self.city, '{year}',
                                '{month:02d}', '{day:02d}', '{ts}.json')
        else:
            raise ValueError(("{} is an unknown city.".format(self.city)))

    @property
    def url(self):
        if self.city == 'bordeaux':
            return BORDEAUX_BIKEAVAILABILITY_URL.format(key=config['bordeaux']['key'])
        elif self.city == 'lyon':
            return LYON_BIKEAVAILABILITY_URL
        else:
            raise ValueError(("{} is an unknown city.".format(self.city)))

    def requires(self):
        return NormalizeStationTable(self.city)

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime("%HH%M") # 16H35
        return luigi.LocalTarget(self.path.format(year=year, month=month, day=day, ts=ts), format=UTF8)

    def run(self):
        resp = requests.get(self.url)
        with self.output().open('w') as fobj:
            if self.city == 'bordeaux':
                fobj.write(resp.content.decode('ISO-8859-1').encode('utf-8').decode('utf-8'))
            elif self.city == 'lyon':
                data = resp.json()
                json.dump(resp.json(), fobj, ensure_ascii=False)
            else:
                raise ValueError(("{} is an unknown city.".format(self.city)))


class AvailabilityToCSV(luigi.Task):
    """Turn real-time bike availability to CSV files
    """
    city = luigi.Parameter()
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)

    @property
    def path(self):
        return os.path.join(DATADIR, self.city, '{year}',
                            '{month:02d}', '{day:02d}', '{ts}.csv')

    def requires(self):
        return BikeAvailability(self.city)

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime('%HH%M') # 16H35
        return luigi.LocalTarget(self.path.format(year=year, month=month,
                                                  day=day, ts=ts, format=UTF8))

    def run(self):
        with self.input().open() as fobj:
            if self.city == 'bordeaux':
                tree = etree.parse(fobj)
                wfs_ns = '{http://www.opengis.net/wfs/2.0}'
                bm_ns = '{http://data.bordeaux-metropole.fr/wfs}'
                elements = (node.find(bm_ns + 'CI_VCUB_P') for node in tree.findall(wfs_ns + 'member'))
                data = []
                for node in elements:
                    data.append(extract_xml_feature(node))
                df = pd.DataFrame([dict(x) for x in data])
                df = df.sort_values(by="ident")
            elif self.city == 'lyon':
                data = json.load(fobj)
                df = pd.DataFrame(data['values'], columns=data['fields'])
            else:
                raise ValueError(("{} is an unknown city.".format(self.city)))
        df = df[[config[self.city]['feature_avl_id'],
                 config[self.city]['feature_timestamp'],
                 config[self.city]['feature_avl_stands'],
                 config[self.city]['feature_avl_bikes'],
                 config[self.city]['feature_status']]]
        df.columns = ["id", "timestamp", "available_stands",
                      "available_bikes", "status"]
        with self.output().open('w') as fobj:
            df.to_csv(fobj, index=False)


class AvailabilityToDB(CopyToTable):
    """Insert bike availability data into a PostgreSQL table
    """
    city = luigi.Parameter()
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)

    host = config['database']['host']
    database = config['database']['dbname']
    user = config['database']['user']
    password = None

    columns = [('id', 'INT'),
               ('timestamp', 'TIMESTAMP'),
               ('available_stands', 'INT'),
               ('available_bikes', 'INT'),
               ('status', 'VARCHAR(12)')]

    @property
    def table(self):
        return '{schema}.timeseries'.format(schema=self.city)

    def rows(self):
        """overload the rows method to skip the first line (header)
        """
        with self.input().open('r') as fobj:
            df = pd.read_csv(fobj)
            for idx, row in df.iterrows():
                yield row.values

    def requires(self):
        return AvailabilityToCSV(self.city, self.timestamp)

    def rows(self):
        """overload the rows method to skip the first line (header)
        """
        with self.input().open('r') as fobj:
            df = pd.read_csv(fobj)
            for idx, row in df.iterrows():
                if row.status == 'None' or row.available_stands == 'None':
                    continue
                yield row.values


class AggregateTransaction(luigi.Task):
    """Aggregate shared-bike transactions data into a CSV file (one transaction
    = one bike taken, or one bike dropped off).
    """
    city = luigi.Parameter()
    date = luigi.DateParameter(default=yesterday())

    @property
    def path(self):
        return os.path.join(DATADIR, self.city, '{year}',
                            '{month:02d}', '{day:02d}', 'transactions.csv')

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.date)
        return luigi.LocalTarget(self.path.format(year=year, month=month, day=day), format=UTF8)

    def run(self):
        query = ("SELECT DISTINCT * FROM {schema}.timeseries "
                 "WHERE timestamp >= %(start)s AND timestamp < %(stop)s "
                 "ORDER BY timestamp, id"
                 ";").format(schema=self.city)
        eng = db()
        query_params = {"start": self.date,
                        "stop": self.date + timedelta(1)}
        df = pd.io.sql.read_sql_query(query, eng, params=query_params)
        transactions = (df.query("status == 'OPEN' or status == 'CONNECTEE'")
                        .groupby("id")['available_bikes']
                        .apply(lambda s: s.diff().abs().sum())
                        .dropna()
                        .to_frame()
                        .reset_index())
        transactions = transactions.rename_axis({"available_bikes": "transactions"}, axis=1)
        with self.output().open('w') as fobj:
            transactions.to_csv(fobj, index=False)


class TransactionsIntoDB(CopyToTable):
    """Copy shared-bike transaction data into the database
    """
    city = luigi.Parameter()
    date = luigi.DateParameter(default=yesterday())

    host = config['database']['host']
    database = config['database']['dbname']
    user = config['database']['user']
    password = None

    columns = [('id', 'INT'),
               ('number', 'FLOAT'),
               ('date', 'DATE')]

    @property
    def table(self):
        return '{schema}.daily_transaction'.format(schema=self.city)

    def rows(self):
        """overload the rows method to skip the first line (header) and add date value
        """
        with self.input().open('r') as fobj:
            next(fobj)
            for line in fobj:
                yield line.strip('\n').split(',') + [self.date]

    def requires(self):
        return AggregateTransaction(self.city, self.date)


class ComputeClusters(luigi.Task):
    """Compute clusters corresponding to bike availability in `city` stations
    between a `start` and an `end` date
    """
    city = luigi.Parameter()
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())

    def outputpath(self):
        fname = "kmeans-{}-to-{}.h5".format(self.start, self.stop)
        return os.path.join(DATADIR, self.city, 'clustering', fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def run(self):
        query = ("SELECT id, timestamp, available_bikes "
                 "FROM {}.timeseries "
                 "WHERE timestamp >= %(start)s "
                 "AND timestamp < %(stop)s;"
                 "").format(self.city)
        eng = db()
        df = pd.io.sql.read_sql_query(query, eng,
                                      params={"start": self.start,
                                              "stop": self.stop})
        df.columns = ["station_id", "ts", "nb_bikes"]
        clusters = compute_clusters(df)
        self.output().makedirs()
        path = self.output().path
        clusters['labels'].to_hdf(path, '/clusters')
        clusters['centroids'].to_hdf(path, '/centroids')


class StoreClustersToDatabase(CopyToTable):
    """Read the cluster labels from `DATADIR/<city>/clustering.h5` file and store
    them into `clustered_stations`

    """
    city = luigi.Parameter()
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())

    host = config['database']['host']
    database = config['database']['dbname']
    user = config['database']['user']
    password = None

    columns = [('station_id', 'INT'),
               ('start', 'DATE'),
               ('stop', 'DATE'),
               ('cluster_id', 'INT')]

    @property
    def table(self):
        return '{schema}.{tablename}'.format(
            schema=self.city,
            tablename=config[self.city]['clustering'])

    def rows(self):
        inputpath = self.input().path
        clusters = pd.read_hdf(inputpath, 'clusters')
        for _, row in clusters.iterrows():
            modified_row = list(row.values)
            modified_row.insert(1, self.stop)
            modified_row.insert(1, self.start)
            yield modified_row

    def requires(self):
        return ComputeClusters(self.city, self.start, self.stop)

    def create_table(self, connection):
        if len(self.columns[0]) == 1:
            # only names of columns specified, no types
            raise NotImplementedError(("create_table() not implemented for %r "
                                       "and columns types not specified")
                                      % self.table)
        elif len(self.columns[0]) == 2:
            # if columns is specified as (name, type) tuples
            coldefs = ','.join('{name} {type}'.format(name=name, type=type)
                               for name, type in self.columns)
            query = ("CREATE TABLE {table} ({coldefs}, "
                     "PRIMARY KEY (station_id, start, stop));"
                     "").format(table=self.table, coldefs=coldefs)
            connection.cursor().execute(query)


class StoreCentroidsToDatabase(CopyToTable):
    """Read the cluster centroids from `DATADIR/<city>/clustering.h5` file and
    store them into `centroids`

    """
    city = luigi.Parameter()
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())

    host = config['database']['host']
    database = config['database']['dbname']
    user = config['database']['user']
    password = None
    first_columns = [('cluster_id', 'INT'),
                     ('start', 'DATE'),
                     ('stop', 'DATE')]

    @property
    def columns(self):
        if len(self.first_columns) == 3:
            self.first_columns.extend([('h'+str(i), 'DOUBLE PRECISION')
                                      for i in range(24)])
        return self.first_columns

    @property
    def table(self):
        return '{schema}.{tablename}'.format(
            schema=self.city,
            tablename=config[self.city]['centroids'])

    def rows(self):
        inputpath = self.input().path
        clusters = pd.read_hdf(inputpath, 'centroids')
        for _, row in clusters.iterrows():
            modified_row = list(row.values)
            modified_row[0] = int(modified_row[0])
            modified_row.insert(1, self.stop)
            modified_row.insert(1, self.start)
            yield modified_row

    def requires(self):
        return ComputeClusters(self.city, self.start, self.stop)

    def create_table(self, connection):
        if len(self.columns[0]) == 1:
            # only names of columns specified, no types
            raise NotImplementedError(("create_table() not implemented for %r "
                                       "and columns types not specified")
                                      % self.table)
        elif len(self.columns[0]) == 2:
            # if columns is specified as (name, type) tuples
            coldefs = ','.join('{name} {type}'.format(name=name, type=type)
                               for name, type in self.columns)
            query = ("CREATE TABLE {table} ({coldefs}, "
                     "PRIMARY KEY (cluster_id, start, stop));"
                     "").format(table=self.table, coldefs=coldefs)
            connection.cursor().execute(query)


class Clustering(luigi.Task):
    """Clustering master task

    """
    city = luigi.Parameter()
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())

    def requires(self):
        yield StoreClustersToDatabase(self.city, self.start, self.stop)
        yield StoreCentroidsToDatabase(self.city, self.start, self.stop)


class TrainXGBoost(luigi.Task):
    """Train a XGBoost model between `start` and `stop` dates to predict bike
    availability at each station in `city`

    Attributes
    ----------
    city : luigi.Parameter
        City of interest, *e.g.* Bordeaux or Lyon
    start : luigi.DateParameter
        Training start date
    stop : luigi.DataParameter
        Training stop date upper bound (actually the end date is computed with
    `validation`)
    validation : luigi.DateMinuteParameter
        Date that bounds the training set and the validation set during the
    XGBoost model training
    frequency : DateOffset, timedelta or str
        Indicates the prediction frequency
    """
    city = luigi.Parameter()
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())
    validation = luigi.DateMinuteParameter(default=dt.now() - timedelta(hours=1))
    frequency = luigi.Parameter(default="30T")

    def outputpath(self):
        fname = "{}-to-{}-at-{}-freq-{}.model".format(self.start, self.stop,
                                           self.validation.isoformat(),
                                           self.frequency)
        return os.path.join(DATADIR, self.city, 'xgboost-model', fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def run(self):
        query = ("SELECT DISTINCT id AS station_id, timestamp AS ts, "
                 "available_bikes AS nb_bikes, available_stands AS nb_stands, "
                 "available_bikes::float / (available_bikes::float "
                 "+ available_stands::float) AS probability "
                 "FROM {schema}.timeseries "
                 "WHERE timestamp >= %(start)s "
                 "AND timestamp < %(stop)s "
                 "AND (available_bikes > 0 OR available_stands > 0) "
                 "AND (status = 'CONNECTEE' OR status = 'OPEN')"
                 "ORDER BY id, timestamp"
                 ";").format(schema=self.city)
        eng = db()
        df = pd.io.sql.read_sql_query(query, eng,
                                      params={"start": self.start,
                                              "stop": self.stop})
        if df.empty:
            raise Exception("There is not any data to process in the DataFrame. "
                            + "Please check the dates.")
        prediction_model = train_prediction_model(df, self.validation, self.frequency)
        self.output().makedirs()
        prediction_model.save_model(self.output().path)
