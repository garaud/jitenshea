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


@app.route('/doc/')
def swagger_ui():
    return apidoc.ui_for(api)

@api.route("/city")
class City(Resource):
    @api.doc("List of cities")
    def get(self):
        return controller.cities()

@api.route("/lyon/station")
class LyonStationList(Resource):
    @api.doc(parser=station_list_parser,
                 description="Bicycle-sharing stations for Lyon")
    def get(self):
        args = station_list_parser.parse_args()
        limit = args['limit']
        return controller.stations('lyon', limit)

@api.route("/lyon/station/<list:ids>")
class LyonStation(Resource):
    @api.doc(description="Bicycle station(s) for Lyon")
    def get(self, ids):
        rset = controller.lyon(ids)
        if not rset:
            api.abort(404, "No such id: {}".format(ids))
        return rset

@api.route("/bordeaux/station")
class BordeauxStationList(Resource):
    @api.doc(parser=station_list_parser,
             description="Bicycle-sharing stations for Bordeaux")
    def get(self):
        args = station_list_parser.parse_args()
        limit = args['limit']
        return controller.stations('bordeaux', limit)

@api.route("/bordeaux/station/<list:ids>")
class BordeauxStation(Resource):
    @api.doc(description="Bicycle station(s) for Bordeaux")
    def get(self, ids):
        rset = controller.bordeaux(ids)
        if not rset:
            api.abort(404, "No such id: {}".format(ids))
        return rset

# URL timeseries/?start=2017-07-21&stop=2017-07-23&limit=10

# série temporelle pour une station
# série temporelle pour une liste de stations ?
# série temporelle entre deux dates, pour une date
# série temporelle entre deux timestamps ??
# daily transaction pour une station, pour plusieurs, triées dans l'ordre décroissant ?

@api.route("/bordeaux/timeseries/<int:year>/<int:month>/<int:day>")
class DailyBordeauxTimeseries(Resource):
    @api.doc(description="One-day timeseries bicycle station for Bordeaux")
    def get(self, year, month, day):
        return []


if __name__ == '__main__':
    app.run(debug=True)

