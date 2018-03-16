# coding: utf-8

"""Flask API for Jitenshea (Bicycle-sharing data)
"""

import daiquiri
import logging

from datetime import date, datetime
from dateutil.parser import parse

from werkzeug.routing import BaseConverter

from flask import abort, jsonify, request, render_template
from flask.json import JSONEncoder
from flask_restplus import fields, inputs
from flask_restplus import Resource, Api

from jitenshea import controller
from jitenshea.webapp import app


ISO_DATE = '%Y-%m-%d'
ISO_DATETIME = '%Y-%m-%dT%H:%M:%S'
CITIES = ('lyon', 'bordeaux')

daiquiri.setup(level=logging.INFO)
logger = daiquiri.getLogger("jitenshea-webapi")

class CustomJSONEncoder(JSONEncoder):
    """Custom JSON encoder to handle date; inherits from `flask.json.JSONEncoder` class
    """
    def default(self, obj):
        """Produces a serialize version of `obj`

        Parameters
        ----------
        obj : object
            Object to serialize into a json file

        Returns
        -------
        obj
            Serializable version of ̀obj`
        """
        try:
            if isinstance(obj, datetime):
                return obj.strftime(ISO_DATETIME)
            if isinstance(obj, date):
                return obj.strftime(ISO_DATE)
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


class ListConverter(BaseConverter):
    """URL <-> Python converter for a list of elements seperated by a ','

    Example URL/user/john,mary to get the john resource and mary resource
    Inspired from http://exploreflask.com/en/latest/views.html#custom-converters
    """
    def to_python(self, value):
        """Convert `value` into a Python list

        Parameters
        ----------
        value : str
            URL to decode
        """
        return value.split(',')

    def to_url(self, values):
        """Convert `values` into a URL

        Parameters
        ----------
        values : list
            List of element to integrate into an URL string
        """
        return ','.join(BaseConverter.to_url(value)
                        for value in values)

app.url_map.converters['list'] = ListConverter
app.json_encoder = CustomJSONEncoder


def parse_date(strdate):
    """Parse a string and convert it to a date

    Parameters
    ----------
    strdate : str
        Date under a string format

    Returns
    -------
    datetime.date
        Converted date
    """
    try:
        year, month, day = [int(x) for x in strdate.split('-')]
        day = date(year, month, day)
    except Exception as e:
        api.abort(422, "date from the request cannot be parsed: {}".format(e))
    return day

def parse_timestamp(str_timestamp):
    """Parse a string and convert it to a datetime

    ISO 8601 format, i.e.
      - YYYY-MM-DD
      - YYYY-MM-DDThh
      - YYYY-MM-DDThhmm

    Parameters
    ----------
    str_timestamp : str
        Timestamp under the string format
    Returns
    -------
    datetime.datetime
        Converted time
    """
    try:
        dt = parse(str_timestamp)
    except Exception as e:
        api.abort(422, "date from the request cannot be parsed: {}".format(e))
    return dt

def check_city(city):
    """City checker, if `city` is not into the list of known cities, it makes
    the API abort

    Parameters
    ----------
    city : str
        City to consider and to compare with the known list
    """
    if city not in CITIES:
        api.abort(404, "City {} not found".format(city))

api = Api(title='Jitenshea: Bicycle-sharing data analysis',
          prefix='/api',
          doc=False,
          version='0.1',
          description="Retrieve some data related to bicycle-sharing data from some cities.")

# Parsers
station_list_parser = api.parser()
station_list_parser.add_argument("limit", required=False, type=int, default=100,
                                 dest='limit', location='args', help='Limit')
station_list_parser.add_argument("geojson", required=False, default=False, dest='geojson',
                                 location='args', help='GeoJSON format?')

daily_parser = api.parser()
daily_parser.add_argument("date", required=True, dest="date", location="args",
                          help="day of the transactions (YYYY-MM-DD)")
daily_parser.add_argument("window", required=False, type=int, default=0, dest="window",
                          location="args", help="How many days?")
daily_parser.add_argument("backward", required=False, type=inputs.boolean, default=True, dest="backward",
                          location="args", help="Backward window of days or not?")

daily_list_parser = api.parser()
daily_list_parser.add_argument("limit", required=False, type=int, default=20,
                               dest='limit', location='args', help='Limit')
daily_list_parser.add_argument("by", required=False, dest='order_by', default='station',
                               location='args', help="Order by 'station' or 'value'")
daily_list_parser.add_argument("date", required=True, dest="date", location="args",
                               help="day of the transactions (YYYY-MM-DD)")
daily_list_parser.add_argument("window", required=False, type=int, default=0,
                               dest="window", location="args", help="How many days?")
daily_list_parser.add_argument("backward", required=False, type=inputs.boolean,
                               default=True, dest="backward", location="args",
                               help="Backward window of days or not?")

timeseries_parser = api.parser()
timeseries_parser.add_argument("start", required=True, dest="start", location="args",
                          help="Start date YYYY-MM-DDThhmm")
timeseries_parser.add_argument("stop", required=True, dest="stop", location="args",
                          help="Stop date YYYY-MM-DDThhmm")

hourly_profile_parser = api.parser()
hourly_profile_parser.add_argument("date", required=True, dest="date", location="args",
                                   help="day of the transactions (YYYY-MM-DD)")
hourly_profile_parser.add_argument("window", required=False, type=int, default=7, dest="window",
                                   location="args", help="How many backward days?")

daily_profile_parser = api.parser()
daily_profile_parser.add_argument("date", required=True, dest="date", location="args",
                                  help="day of the transactions (YYYY-MM-DD)")
daily_profile_parser.add_argument("window", required=False, type=int, default=30, dest="window",
                                   location="args", help="How many backward days?")

clustering_parser = api.parser()
clustering_parser.add_argument("geojson", required=False, type=inputs.boolean,
                               default=False, dest='geojson', location='args',
                               help='GeoJSON format?')


@api.route("/city")
class City(Resource):
    """Set up the Flask Resource dedicated to city requesting
    """
    @api.doc("List of cities")
    def get(self):
        """Get the list of considered cities

        Returns
        -------
        flask.Response
            Jsonified version of the city list
        """
        return jsonify(controller.cities())

@api.route("/<string:city>/station")
class CityStationList(Resource):
    """Set up the Flask Resource dedicated to station list requesting, for each
    considered city
    """
    @api.doc(parser=station_list_parser,
                 description="Bicycle-sharing stations")
    def get(self, city):
        """Get the station list for a given `city`

        Parameters
        ----------
        city : str
            City to consider, either `bordeaux` or `lyon`

        Returns
        -------
        flask.Response
            Jsonified version of the list of stations in `city`
        """
        check_city(city)
        args = station_list_parser.parse_args()
        limit = args['limit']
        geojson = args['geojson']
        return jsonify(controller.stations(city, limit, geojson))

@api.route("/<string:city>/station/<list:ids>")
class CityStation(Resource):
    """Set up the Flask Resource dedicated to station requesting, for each
    considered city
    """
    @api.doc(description="Bicycle station(s)")
    def get(self, city, ids):
        """Get the station(s) identified by `ids` for a given `city`

        Parameters
        ----------
        city : str
            City to consider, either `bordeaux` or `lyon`
        ids : list of integers
            IDs of station to consider in `city`

        Returns
        -------
        flask.Response
            Jsonified version of stations in `city`
        """
        check_city(city)
        rset = controller.specific_stations(city, ids)
        if not rset:
            api.abort(404, "No such id: {}".format(ids))
        return jsonify(rset)

@api.route("/<string:city>/daily/station/<list:ids>")
class CityDailyStation(Resource):
    """Set up the Flask Resource dedicated to daily transaction requesting, for
    each station in each considered city
    """
    @api.doc(parser=daily_parser,
             description="Bicycle station(s) daily transactions")
    def get(self, city, ids):
        """Get the daily transactions that occurred on the station(s)
        identified by `ids` for a given `city`

        Parameters
        ----------
        city : str
            City to consider, either `bordeaux` or `lyon`
        ids : list of integers
            IDs of station to consider in `city`

        Returns
        -------
        flask.Response
            Jsonified version of station daily transaction
        """
        check_city(city)
        args = daily_parser.parse_args()
        day = parse_date(args['date'])
        rset = controller.daily_transaction(city, ids, day, args['window'],
                                            args['backward'])
        if not rset:
            api.abort(404, "No such data for id: {} at {}".format(ids, day))
        return jsonify(rset)


@api.route("/<string:city>/daily/station")
class CityDailyStationList(Resource):
    """Set up the Flask Resource dedicated to total daily transaction
    requesting, for each considered city
    """
    @api.doc(parser=daily_list_parser,
             description="Daily transactions for all stations")
    def get(self, city):
        """Get the total daily transactions that occurred in a given `city`

        Parameters
        ----------
        city : str
            City to consider, either `bordeaux` or `lyon`

        Returns
        -------
        flask.Response
            Jsonified version of city daily transaction
        """
        check_city(city)
        args = daily_list_parser.parse_args()
        day = parse_date(args['date'])
        limit = args['limit']
        order_by = args['order_by']
        if order_by not in ('station', 'value'):
            api.abort(400, "wrong 'by' value parameter. Should be 'station' of 'value'")
        rset = controller.daily_transaction_list(city, day, limit, order_by,
                                                 args['window'], args['backward'])
        return jsonify(rset)


@api.route("/<string:city>/timeseries/station/<list:ids>")
class CityTimeseriesStation(Resource):
    """Set up the Flask Resource dedicated to station timeseries requesting,
    for each station, in each considered city
    """
    @api.doc(parser=timeseries_parser,
             description="Bicycle station(s) timeseries")
    def get(self, city, ids):
        check_city(city)
        args = timeseries_parser.parse_args()
        start = parse_timestamp(args['start'])
        stop = parse_timestamp(args['stop'])
        rset = controller.timeseries(city, ids, start, stop)
        if not rset:
            api.abort(404, "No such data for id: {} between {} and {}".format(ids, start, stop))
        return jsonify(rset)

@api.route("/<string:city>/profile/hourly/station/<list:ids>")
class CityHourlyStation(Resource):
    """Set up the Flask Resource dedicated to bicycle station hourly profile
    requesting, for each station in each considered city
    """
    @api.doc(parser=hourly_profile_parser,
             description="Bicycle station(s) hourly profile")
    def get(self, city, ids):
        """Get the bike availability hourly profile on the station(s)
        identified by `ids` for a given `city`

        Parameters
        ----------
        city : str
            City to consider, either `bordeaux` or `lyon`
        ids : list of integers
            IDs of station to consider in `city`

        Returns
        -------
        flask.Response
            Jsonified version of station hourly profile
        """
        check_city(city)
        args = hourly_profile_parser.parse_args()
        day = parse_date(args['date'])
        window = args['window']
        rset = controller.hourly_profile(city, ids, day, window)
        if not rset:
            api.abort(404, "No such data for id: {} for {}".format(ids, day))
        return jsonify(rset)

@api.route("/<string:city>/profile/daily/station/<list:ids>")
class CityDailyStation(Resource):
    """Set up the Flask Resource dedicated to bicycle station daily profile
    requesting, for each station in each considered city
    """
    @api.doc(parser=daily_profile_parser,
             description="Bicycle station(s) daily profile")
    def get(self, city, ids):
        """Get the hourly transactions that occurred on the station(s)
        identified by `ids` for a given `city`

        Parameters
        ----------
        city : str
            City to consider, either `bordeaux` or `lyon`
        ids : list of integers
            IDs of station to consider in `city`

        Returns
        -------
        flask.Response
            Jsonified version of station daily profile
        """
        check_city(city)
        args = daily_profile_parser.parse_args()
        day = parse_date(args['date'])
        window = args['window']
        rset = controller.daily_profile(city, ids, day, window)
        if not rset:
            api.abort(404, "No such data for id: {} for {}".format(ids, day))
        return jsonify(rset)


@api.route("/<string:city>/clustering/stations")
class CityClusteredStation(Resource):
    @api.doc(parser=clustering_parser,
             description="Clustered stations according to K-means algorithm")
    def get(self, city):
        check_city(city)
        args = clustering_parser.parse_args()
        rset = controller.station_clusters(city, geojson=args['geojson'])
        if not rset:
            api.abort(404, ("No K-means algorithm trained in this city"))
        return jsonify(rset)


@api.route("/<string:city>/clustering/centroids")
class CityClusterCentroids(Resource):
    @api.doc(description="Centroids of clusters computed with a K-means algorithm")
    def get(self, city):
        check_city(city)
        rset = controller.cluster_profiles(city)
        if not rset:
            api.abort(404, ("No K-means algorithm trained in this city"))
        return jsonify(rset)

@api.route("/<string:city>/clustering/centroids")
class CityClusterCentroids(Resource):
    @api.doc(parser=clustering_parser,
             description="Centroids of clusters computed with a K-means algorithm")
    def get(self, city):
        check_city(city)
        args = clustering_parser.parse_args()
        start_date = parse_date(args["start_date"])
        window = args["window"]
        rset = controller.cluster_profiles(city, start_date, window)
        if not rset:
            api.abort(404, ("No K-means algorithm trained between {} and {}"
                            "").format(start_date, start_date + window))
        return jsonify(rset)
