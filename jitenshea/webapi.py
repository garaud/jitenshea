# coding: utf-8

"""Flask API for Jitenshea (Bicycle-sharing data)
"""

import daiquiri
import logging

from datetime import date, datetime
from dateutil.parser import parse

from werkzeug.routing import BaseConverter

from flask import Flask, jsonify, render_template
from flask.json import JSONEncoder
from flask_restplus import fields, inputs
from flask_restplus import Resource, Api, apidoc

from jitenshea import controller

ISO_DATE = '%Y-%m-%d'
ISO_DATETIME = '%Y-%m-%dT%H:%M:%S'

daiquiri.setup(level=logging.INFO)
logger = daiquiri.getLogger("jitenshea")


app = Flask(__name__)
app.config['ERROR_404_HELP'] = False
app.config['SWAGGER_UI_DOC_EXPANSION'] = 'list'

class CustomJSONEncoder(JSONEncoder):
    """Custom JSON encoder to handle date
    """
    def default(self, obj):
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
        return value.split(',')

    def to_url(self, values):
        return ','.join(BaseConverter.to_url(value)
                        for value in values)

app.url_map.converters['list'] = ListConverter
app.json_encoder = CustomJSONEncoder


def parse_date(strdate):
    """Parse a string and convert it to a date
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
    """
    try:
        dt = parse(str_timestamp)
    except Exception as e:
        api.abort(422, "date from the request cannot be parsed: {}".format(e))
    return dt


@app.route('/')
def index():
    return render_template("index.html")

api = Api(app,
          title='Jitenshea: Bicycle-sharing data analysis',
          ui=False,
          version='0.1',
          description="Retrieve some data related to bicycle-sharing data from some cities.")

# Parsers
station_list_parser = api.parser()
station_list_parser.add_argument("limit", required=False, default=20, dest='limit',
                                 location='args', help='Limit')
daily_parser = api.parser()
daily_parser.add_argument("date", required=True, dest="date", location="args",
                          help="day of the transactions")
daily_parser.add_argument("window", required=False, type=int, default=0, dest="window",
                          location="args", help="How many days?")
daily_parser.add_argument("backward", required=False, type=inputs.boolean, default=True, dest="backward",
                          location="args", help="Backward window of days or not?")

daily_list_parser = api.parser()
daily_list_parser.add_argument("limit", required=False, default=20, dest='limit',
                               location='args', help='Limit')
daily_list_parser.add_argument("by", required=False, dest='order_by', default='station',
                               location='args', help="Order by 'station' or 'value'")
daily_list_parser.add_argument("date", required=True, dest="date", location="args",
                               help="day of the transactions")
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
                                   help="day of the transactions")
hourly_profile_parser.add_argument("window", required=False, type=int, default=7, dest="window",
                                   location="args", help="How many backward days?")


@app.route('/doc/')
def swagger_ui():
    return apidoc.ui_for(api)

@api.route("/city")
class City(Resource):
    @api.doc("List of cities")
    def get(self):
        return jsonify(controller.cities())

@api.route("/lyon/station")
class LyonStationList(Resource):
    @api.doc(parser=station_list_parser,
                 description="Bicycle-sharing stations for Lyon")
    def get(self):
        args = station_list_parser.parse_args()
        limit = args['limit']
        return jsonify(controller.stations('lyon', limit))

@api.route("/lyon/station/<list:ids>")
class LyonStation(Resource):
    @api.doc(description="Bicycle station(s) for Lyon")
    def get(self, ids):
        rset = controller.lyon(ids)
        if not rset:
            api.abort(404, "No such id: {}".format(ids))
        return jsonify(rset)

@api.route("/bordeaux/station")
class BordeauxStationList(Resource):
    @api.doc(parser=station_list_parser,
             description="Bicycle-sharing stations for Bordeaux")
    def get(self):
        args = station_list_parser.parse_args()
        limit = args['limit']
        return jsonify(controller.stations('bordeaux', limit))

@api.route("/bordeaux/station/<list:ids>")
class BordeauxStation(Resource):
    @api.doc(description="Bicycle station(s) for Bordeaux")
    def get(self, ids):
        rset = controller.bordeaux(ids)
        if not rset:
            api.abort(404, "No such id: {}".format(ids))
        return jsonify(rset)

@api.route("/bordeaux/daily/station/<list:ids>")
class BordeauxDailyStation(Resource):
    @api.doc(parser=daily_parser,
             description="Bicycle station(s) daily transactions for Bordeaux")
    def get(self, ids):
        args = daily_parser.parse_args()
        day = parse_date(args['date'])
        rset = controller.daily_transaction('bordeaux', ids, day,
                                            args['window'], args['backward'])
        if not rset:
            api.abort(404, "No such data for id: {} at {}".format(ids, day))
        return jsonify(rset)

@api.route("/lyon/daily/station/<list:ids>")
class LyonDailyStation(Resource):
    @api.doc(parser=daily_parser,
             description="Bicycle station(s) daily transactions for Lyon")
    def get(self, ids):
        args = daily_parser.parse_args()
        day = parse_date(args['date'])
        rset = controller.daily_transaction('lyon', ids, day, args['window'],
                                            args['backward'])
        if not rset:
            api.abort(404, "No such data for id: {} at {}".format(ids, day))
        return jsonify(rset)


@api.route("/bordeaux/daily/station")
class BordeauxDailyStationList(Resource):
    @api.doc(parser=daily_list_parser,
             description="Daily transactions for all stations in Bordeaux")
    def get(self):
        args = daily_list_parser.parse_args()
        day = parse_date(args['date'])
        limit = args['limit']
        order_by = args['order_by']
        if order_by not in ('station', 'value'):
            api.abort(400, "wrong 'by' value parameter. Should be 'station' of 'value'")
        rset = controller.daily_transaction_list('bordeaux', day, limit, order_by,
                                                 args['window'], args['backward'])
        return jsonify(rset)

@api.route("/lyon/daily/station")
class BordeauxDailyStationList(Resource):
    @api.doc(parser=daily_list_parser,
             description="Daily transactions for all stations in Lyon")
    def get(self):
        args = daily_list_parser.parse_args()
        day = parse_date(args['date'])
        limit = args['limit']
        order_by = args['order_by']
        if order_by not in ('station', 'value'):
            api.abort(400, "wrong 'by' value parameter. Should be 'station' of 'value'")
        rset = controller.daily_transaction_list('lyon', day, limit, order_by,
                                                 args['window'], args['backward'])
        return jsonify(rset)


@api.route("/bordeaux/timeseries/station/<list:ids>")
class BordeauxDailyStation(Resource):
    @api.doc(parser=timeseries_parser,
             description="Bicycle station(s) timeseries for Bordeaux")
    def get(self, ids):
        args = timeseries_parser.parse_args()
        start = parse_timestamp(args['start'])
        stop = parse_timestamp(args['stop'])
        rset = controller.timeseries('bordeaux', ids, start, stop)
        if not rset:
            api.abort(404, "No such data for id: {} between {} and {}".format(ids, start, stop))
        return jsonify(rset)


@api.route("/lyon/timeseries/station/<list:ids>")
class LyonDailyStation(Resource):
    @api.doc(parser=timeseries_parser,
             description="Bicycle station(s) timeseries for Lyon")
    def get(self, ids):
        args = timeseries_parser.parse_args()
        start = parse_timestamp(args['start'])
        stop = parse_timestamp(args['stop'])
        rset = controller.timeseries('lyon', ids, start, stop)
        if not rset:
            api.abort(404, "No such data for id: {} between {} and {}".format(ids, start, stop))
        return jsonify(rset)


@api.route("/bordeaux/profile/hourly/station/<list:ids>")
class BordeauxHourlyStation(Resource):
    @api.doc(parser=hourly_profile_parser,
             description="Bicycle station(s) hourly profile for Bordeaux")
    def get(self, ids):
        args = hourly_profile_parser.parse_args()
        day = parse_date(args['date'])
        window = args['window']
        rset = controller.hourly_profile('bordeaux', ids, day, window)
        if not rset:
            api.abort(404, "No such data for id: {} for {}".format(ids, day))
        return jsonify(rset)


@api.route("/lyon/profile/hourly/station/<list:ids>")
class LyonHourlyStation(Resource):
    @api.doc(parser=hourly_profile_parser,
             description="Bicycle station(s) hourly profile for Lyon")
    def get(self, ids):
        args = hourly_profile_parser.parse_args()
        day = parse_date(args['date'])
        window = args['window']
        rset = controller.hourly_profile('lyon', ids, day, window)
        if not rset:
            api.abort(404, "No such data for id: {} for {}".format(ids, day))
        return jsonify(rset)


if __name__ == '__main__':
    app.run(debug=True)

