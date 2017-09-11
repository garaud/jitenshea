# coding: utf-8

"""Flask API for Jitenshea (Bicycle-sharing data)
"""

import daiquiri
import logging

from datetime import date

from werkzeug.routing import BaseConverter

from flask import Flask, jsonify, render_template
from flask.json import JSONEncoder
from flask_restplus import fields, inputs
from flask_restplus import Resource, Api, apidoc

from jitenshea import controller

ISO_DATE = '%Y-%m-%d'

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


# URL timeseries/?start=2017-07-21&stop=2017-07-23&limit=10

# série temporelle pour une station
# série temporelle pour une liste de stations ?
# série temporelle entre deux dates, pour une date
# série temporelle entre deux timestamps ??
# daily transaction pour une station, pour plusieurs, triées dans l'ordre décroissant ?

# @api.route("/bordeaux/timeseries/<int:year>/<int:month>/<int:day>")
# class DailyBordeauxTimeseries(Resource):
#     @api.doc(description="One-day timeseries bicycle station for Bordeaux")
#     def get(self, year, month, day):
#         return []


if __name__ == '__main__':
    app.run(debug=True)

