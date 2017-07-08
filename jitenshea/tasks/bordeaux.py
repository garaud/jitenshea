# coding: utf-8

"""Luigi tasks to retrieve and process data for Bordeaux.

Note: the 'ident' field which should be used for an unique id for each station
is different when you load the layer TB_STVEL_P and CI_VCUB_P.

  - TB_STVEL_P: bicyle-station geoloc
  - CI_VCUB_P: bicyle-station real-time occupation data

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

import pandas as pd

import luigi
import luigi.postgres
from luigi.format import UTF8, MixedUnicodeBytes

from jitenshea import config
from jitenshea.iodb import db, psql_args, shp2pgsql_args


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
    schema = luigi.Parameter(default=config['bordeaux']["schema"])
    projection = luigi.Parameter(default='4326')
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


class BicycleStationDatabase(luigi.postgres.CopyToTable):
    """Insert VCUB stations data into a PostgreSQL table
    """
    timestamp = luigi.DateMinuteParameter(default=dt.now(), interval=5)

    host = 'localhost'
    database = config['database']['dbname']
    # user = 'tempus'
    user = config['database']['user']
    password = None
    table = '{schema}.{tablename}'.format(schema=config['bordeaux']['schema'],
                                          tablename=config['bordeaux']['table'])
    columns = [('gid', 'INT'),
               ('ident', 'INT'),
               ('type', 'VARCHAR(5)'),
               ('nom', 'VARCHAR(200)'),
               ('etat', 'VARCHAR(20)'),
               ('nbplaces', 'INT'),
               ('nbvelos', 'INT'),
               ('heure', 'TIMESTAMP')]

    def rows(self):
        """overload the rows method to skip the first line (header)
        """
        with self.input().open('r') as fobj:
            df = pd.read_csv(fobj)
            for idx, row in df.iterrows():
                yield row.values

    def requires(self):
        return BicycleStationXMLtoCSV(self.timestamp)
