# coding: utf-8

"""Luigi tasks to retrieve and process data for Bordeaux.

Note: the 'ident' field which should be used for an unique id for each station
is different when you load the layer TB_STVEL_P and CI_VCUB_P.

  - TB_STVEL_P: bicycle-station geoloc
  - CI_VCUB_P: bicycle-station real-time occupation data

So, if you want to merge these data, use the 'numstat' from TB_STVEL_P and
'ident' from CI_VCUB_P.

See also http://data.bordeaux-metropole.fr/dicopub/#/dico#CI_VCUB_P
"""


import os
import zipfile
from datetime import datetime as dt
from datetime import date, timedelta

from lxml import etree

import sh

import requests

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import luigi
from luigi.contrib.postgres import CopyToTable, PostgresQuery
from luigi.format import UTF8, MixedUnicodeBytes

from jitenshea import config
from jitenshea.iodb import db, psql_args, shp2pgsql_args
from jitenshea.stats import compute_clusters, train_prediction_model

# To get shapefile (in a zip).
BORDEAUX_STATION_URL = 'https://data.bordeaux-metropole.fr/files.php?gid=43&format=2'
# Same data as the shapefile but in XML
BORDEAUX_STATION_URL_XML = 'https://data.bordeaux-metropole.fr/wfs?service=wfs&request=GetFeature&version=2.0.0&key={key}&typename=TB_STVEL_P&SRSNAME=EPSG:3945'
BORDEAUX_WFS = 'https://data.bordeaux-metropole.fr/wfs?service=wfs&request=GetFeature&version=2.0.0&key={key}&typename=CI_VCUB_P'
DATADIR = 'datarepo/bordeaux'


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

def collect_xml_station(fobj):
    """Get bicycle stations from XML before inserted them into a Postgres table

    Also get the Geometry Point(3945)
    """
    data = []
    tree = etree.parse(fobj)
    wfs_ns = '{http://www.opengis.net/wfs/2.0}'
    bm_ns = '{http://data.bordeaux-metropole.fr/wfs}'
    elements = (node.find(bm_ns + 'TB_STVEL_P') for node in tree.findall(wfs_ns + 'member'))
    for element in elements:
        # TODO Get the Geom Point
        data.append((element.findtext(bm_ns + "GID"),
                     element.findtext(bm_ns + "NUMSTAT"),
                     element.findtext(bm_ns + "IDENT"),
                     element.findtext(bm_ns + "ADRESSE"),
                     element.findtext(bm_ns + "COMMUNE"),
                     # element.findtext(bm_ns + "DATESERV"),
                     element.findtext(bm_ns + "LIGNCORR"),
                     element.findtext(bm_ns + "NBSUPPOR"),
                     element.findtext(bm_ns + "NOM"),
                     element.findtext(bm_ns + "TARIF"),
                     element.findtext(bm_ns + "TERMBANC"),
                     element.findtext(bm_ns + "TYPEA"),
                     element.findtext(bm_ns + "GEOM"),
                     element.findtext(bm_ns + "CDATE"),
                     element.findtext(bm_ns + "MDATE")))
    return data


class ShapefilesTask(luigi.Task):
    """Task to download a zip files which includes the shapefile
    """
    path = os.path.join(DATADIR , 'vcub.zip')
    srid = 4326

    def output(self):
        return luigi.LocalTarget(self.path, format=MixedUnicodeBytes)

    def run(self):
        with self.output().open('w') as fobj:
            resp = requests.get(BORDEAUX_STATION_URL)
            resp.raise_for_status()
            fobj.write(resp.content)


class UnzipTask(luigi.Task):
    """Task dedicated to unzip file

    To get trace that the task has be done, the task creates a text file with
    the same same of the input zip file with the '.done' suffix. This generated
    file contains the path of the zipfile and all extracted files.
    """
    path = os.path.join(DATADIR , 'vcub.zip')

    def requires(self):
        return ShapefilesTask()

    def output(self):
        filepath = os.path.join(DATADIR, "unzip-" + "vcub" + '.done')
        return luigi.LocalTarget(filepath)

    def run(self):
        with self.output().open('w') as fobj:
            fobj.write("unzip {} at {}\n".format("vcub", dt.now()))
            zip_ref = zipfile.ZipFile(self.path)
            fobj.write("\n".join(elt.filename for elt in zip_ref.filelist))
            fobj.write("\n")
            zip_ref.extractall(DATADIR)
            zip_ref.close()


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


class ShapefileIntoDB(luigi.Task):
    """Dump a shapefile into a table
    """
    schema = luigi.Parameter(default=config['bordeaux']["schema"])
    projection = luigi.Parameter(default='2154')
    fname = "TB_STVEL_P"
    table = "vcub_station"

    def requires(self):
        return {"zip": UnzipTask(),
                "schema": CreateSchema(schema=self.schema)}

    def output(self):
        filepath = '_'.join(['task', 'shp2pgsql', "vcub", "to",
                             self.schema, self.table, 'proj', self.projection])
        return luigi.LocalTarget(os.path.join(DATADIR, filepath + '.txt'))

    def run(self):
        table = self.schema + '.' + self.table
        dirname = os.path.abspath(os.path.dirname(self.input()['zip'].path))
        shpfile = os.path.join(dirname, self.fname + '.shp')
        shp2args = shp2pgsql_args(self.projection, shpfile, table)
        psqlargs = psql_args()
        # check if the schema exist. raise if this is not the case
        with self.output().open('w') as fobj:
            sh.psql(sh.shp2pgsql(shp2args), psqlargs)
            fobj.write("shp2pgsql {} at {}\n".format(shpfile, dt.now()))
            fobj.write("Create {schema}.{table}\n"
                       .format(schema=self.schema, table=self.table))

class BicycleStationGeoXML(luigi.Task):
    """The shapefile from the file.php service seems outdated.

    Download the XML file before to dumpt it into the Database
    """
    filename = "vcub.xml"

    def output(self):
        return luigi.LocalTarget(os.path.join(DATADIR, self.filename), format=UTF8)

    def run(self):
        resp = requests.get(BORDEAUX_STATION_URL_XML.format(key=config['bordeaux']['key']))
        with self.output().open('w') as fobj:
            # Note: I hate ISO-8859-1!!
            fobj.write(resp.content.decode('latin1')
                       .encode('utf-8')
                       .decode('utf-8')
                       .replace("ISO-8859-1", "UTF-8"))


class BicycleStationAvailability(luigi.Task):
    """Get in real-time the shared cycle stations avaibility in a XML format.

    Get data every 5 minutes
    """
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)
    path = os.path.join(DATADIR, '{year}', '{month:02d}', '{day:02d}', '{ts}.xml')

    def requires(self):
        return ShapefileIntoDB()

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime("%HH%M") # 16H35
        return luigi.LocalTarget(self.path.format(year=year, month=month, day=day, ts=ts), format=UTF8)

    def run(self):
        with self.output().open('w') as fobj:
            resp = requests.get(BORDEAUX_WFS.format(key=config['bordeaux']['key']))
            fobj.write(resp.content.decode('ISO-8859-1').encode('utf-8').decode('utf-8'))
            # data = pd.read_csv(BORDEAUX_WFS.format(key=config['bordeaux']['key']))
            # data.columns = [x.lower() for x in data.columns]
            # data['heure'] = data['heure'].apply(lambda x: pd.Timestamp(str(x)))
            # data.to_csv(fobj, index=False)


class BicycleStationXMLtoCSV(luigi.Task):
    """Turn real-time bicycle station XML/WFS data file to a CSV.
    """
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)
    path = os.path.join(DATADIR, '{year}', '{month:02d}', '{day:02d}', '{ts}.csv')
    keepcols = ["gid", "ident", "type", "nom", "etat", "nbplaces", "nbvelos", "heure"]

    def output(self):
        triple = lambda x: (x.year, x.month, x.day)
        year, month, day = triple(self.timestamp)
        ts = self.timestamp.strftime("%HH%M") # 16H35
        return luigi.LocalTarget(self.path.format(year=year, month=month, day=day, ts=ts), format=UTF8)

    def requires(self):
        return BicycleStationAvailability(self.timestamp)

    def run(self):
        with self.input().open() as fobj:
            tree = etree.parse(fobj)
        # Two XML namespaces
        wfs_ns = '{http://www.opengis.net/wfs/2.0}'
        bm_ns = '{http://data.bordeaux-metropole.fr/wfs}'
        elements = (node.find(bm_ns + 'CI_VCUB_P') for node in tree.findall(wfs_ns + 'member'))
        data = []
        for node in elements:
            data.append(extract_xml_feature(node))
        df = pd.DataFrame([dict(x) for x in data])
        df = df.sort_values(by="ident")
        with self.output().open('w') as fobj:
            df[self.keepcols].to_csv(fobj, index=False)


class BicycleStationDatabase(CopyToTable):
    """Insert VCUB stations data into a PostgreSQL table
    """
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)

    host = 'localhost'
    database = config['database']['dbname']
    user = config['database']['user']
    password = None
    table = '{schema}.{tablename}'.format(schema=config['bordeaux']['schema'],
                                          tablename=config['bordeaux']['table'])
    columns = [('gid', 'INT'),
               ('ident', 'INT'),
               ('type', 'VARCHAR(5)'),
               ('name', 'VARCHAR(200)'),
               ('state', 'VARCHAR(12)'),
               ('available_stand', 'INT'),
               ('available_bike', 'INT'),
               ('ts', 'TIMESTAMP')]

    def rows(self):
        """overload the rows method to skip the first line (header)
        """
        with self.input().open('r') as fobj:
            df = pd.read_csv(fobj)
            for idx, row in df.iterrows():
                yield row.values

    def requires(self):
        return BicycleStationXMLtoCSV(self.timestamp)


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
        query = """SELECT DISTINCT ident, type, state, available_bike, ts
           FROM {schema}.{tablename}
           WHERE ts >= %(start)s AND ts < %(stop)s
           ORDER BY ident,ts;""".format(schema=config["bordeaux"]["schema"],
                                        tablename=config['bordeaux']['table'])
        eng = db()
        df = pd.io.sql.read_sql_query(query, eng, params={"start": self.date,
                                                          "stop": self.date + timedelta(1)})
        transactions = (df.query("state == 'CONNECTEE'")
                        .groupby("ident")['available_bike']
                        .apply(lambda s: s.diff().abs().sum())
                        .dropna()
                        .to_frame()
                        .reset_index())
        transactions = transactions.rename_axis({"available_bike": "transactions"}, axis=1)
        with self.output().open('w') as fobj:
            transactions.to_csv(fobj, index=False)


class AggregateVCUBTransactionIntoDB(CopyToTable):
    """Aggregate bicycle-share transactions data into the database.
    """
    date = luigi.DateParameter(default=yesterday())

    host = 'localhost'
    database = config['database']['dbname']
    user = config['database']['user']
    password = None
    table = '{schema}.{tablename}'.format(schema=config['bordeaux']['schema'],
                                          tablename=config['bordeaux']['daily_transaction'])
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

class BordeauxComputeClusters(luigi.Task):
    """Compute clusters corresponding to bike availability in bordeaux stations
    between a `start` and an `end` date

    """
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())

    def outputpath(self):
        start_date = self.start.strftime("%Y%m%d")
        stop_date = self.stop.strftime("%Y%m%d")
        fname = "bordeaux-{}-{}-clustering.h5".format(start_date, stop_date)
        return os.path.join(DATADIR, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def run(self):
        query = ("SELECT gid, ts, available_bike "
                 "FROM {}.{} "
                 "WHERE ts >= %(start)s "
                 "AND ts < %(stop)s;"
                 "").format(config['bordeaux']['schema'], config['bordeaux']['table'])
        eng = db()
        df = pd.io.sql.read_sql_query(query, eng,
                                      params={"start": self.start,
                                              "stop": self.stop})
        df.columns = ["station_id", "ts", "nb_bikes"]
        clusters = compute_clusters(df)
        path = self.output().path
        clusters['labels'].to_hdf(path, '/clusters')
        clusters['centroids'].to_hdf(path, '/centroids')

class BordeauxStoreClustersToDatabase(CopyToTable):
    """Read the cluster labels from `DATADIR/bordeaux-clustering.h5` file and store
    them into `clustered_stations`

    """
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())

    host = 'localhost'
    database = config['database']['dbname']
    user = config['database']['user']
    password = None
    table = '{schema}.{tablename}'.format(schema=config['bordeaux']['schema'],
                                          tablename=config['bordeaux']['clustering'])
    columns = [('station_id', 'INT'),
               ('start', 'DATE'),
               ('stop', 'DATE'),
               ('cluster_id', 'INT')]

    def rows(self):
        inputpath = self.input().path
        clusters = pd.read_hdf(inputpath, 'clusters')
        for _, row in clusters.iterrows():
            modified_row = list(row.values)
            modified_row.insert(1, self.stop)
            modified_row.insert(1, self.start)
            yield modified_row

    def requires(self):
        return BordeauxComputeClusters(self.start, self.stop)

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

class BordeauxStoreCentroidsToDatabase(CopyToTable):
    """Read the cluster centroids from `DATADIR/bordeaux-clustering.h5` file and
    store them into `centroids`

    """
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())

    host = 'localhost'
    database = config['database']['dbname']
    user = config['database']['user']
    password = None
    table = '{schema}.{tablename}'.format(schema=config['bordeaux']['schema'],
                                          tablename=config['bordeaux']['centroids'])
    first_columns = [('cluster_id', 'INT'), ('start', 'DATE'), ('stop', 'DATE')]

    @property
    def columns(self):
        if len(self.first_columns) == 3:
            self.first_columns.extend([('h'+str(i), 'DOUBLE PRECISION')
                                      for i in range(24)])
        return self.first_columns

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
        return BordeauxComputeClusters(self.start, self.stop)

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

class BordeauxClustering(luigi.Task):
    """Clustering master task

    """
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())

    def requires(self):
        yield BordeauxStoreClustersToDatabase(self.start, self.stop)
        yield BordeauxStoreCentroidsToDatabase(self.start, self.stop)

class BordeauxTrainXGBoost(luigi.Task):
    """Train a XGBoost model between `start` and `stop` dates to predict bike
    availability at each station

    Attributes
    ----------
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
    start = luigi.DateParameter(default=yesterday())
    stop = luigi.DateParameter(default=date.today())
    validation = luigi.DateMinuteParameter(default=date.today()-timedelta(hours=1))
    frequency = luigi.Parameter(default="30T")

    def outputpath(self):
        start_date = self.start.strftime("%Y%m%d")
        stop_date = self.stop.strftime("%Y%m%d")
        validation_date = self.validation.strftime("%Y%m%dT%H%M")
        fname = "bordeaux-{}-{}-{}-{}.xgboost.model".format(start_date,
                                                        stop_date,
                                                        validation_date,
                                                        self.frequency)
        return os.path.join(DATADIR, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def run(self):
        query = ("SELECT DISTINCT gid AS station_id, ts, "
                 "available_bike AS nb_bikes, "
                 "available_stand AS nb_stands, "
                 "available_bike::float / (available_bike::float "
                 "+ available_stand::float) AS probability "
                 "FROM {}.{} "
                 "WHERE ts >= %(start)s "
                 "AND ts < %(stop)s "
                 "AND (available_bike > 0 OR available_stand > 0) "
                 "AND state = 'CONNECTEE'"
                 "ORDER BY station_id, ts"
                 ";").format(config['bordeaux']['schema'],
                             config['bordeaux']['table'])
        eng = db()
        df = pd.io.sql.read_sql_query(query, eng,
                                      params={"start": self.start,
                                              "stop": self.stop})
        prediction_model = train_prediction_model(df,
                                                  self.validation,
                                                  self.frequency)
        path = self.output().path
        prediction_model.save_model(path)
