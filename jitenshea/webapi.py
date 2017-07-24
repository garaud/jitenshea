# coding: utf-8

"""Flask API for Jitenshea (Bicycle-sharing data)
"""

import daiquiri
import logging

from flask import Flask, jsonify, render_template
from flask_restplus import fields
from flask.ext.restplus import Resource, Api, apidoc

from jitenshea import controller

daiquiri.setup(level=logging.INFO)
logger = daiquiri.getLogger(__name__)


app = Flask(__name__)


@app.route('/')
def index():
    return render_template("index.html")

api = Api(app,
          title='Jitenshea: Bicycle-sharing data analysis',
          # ui=False,
          version='0.1',
          description="Retrieve some data related to bicycle-sharing data from some cities.")

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
    def get(self):
        return controller.stations('lyon')

@api.route("/lyon/station/<int:id>")
class LyonStation(Resource):
    def get(self, id):
        return {"id": id}

@api.route("/bordeaux/station")
class BordeauxStationList(Resource):
    def get(self):
        return controller.stations('bordeaux')

@api.route("/bordeaux/station/<int:id>")
class BordeauxStation(Resource):
    def get(self, id):
        return {"id": id}

if __name__ == '__main__':
    app.run(debug=True)

