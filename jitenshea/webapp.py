# coding: utf-8

"""Flask Web Application for Jitenshea (Bicycle-sharing data)
"""

import daiquiri
import logging

from flask import Flask, render_template, abort
from flask_restplus import apidoc


daiquiri.setup(level=logging.INFO)
logger = daiquiri.getLogger("jitenshea-webapp")


app = Flask(__name__)

CITIES = ['bordeaux', 'lyon']


def check_city(city):
    if city not in CITIES:
        abort(404, "City {} not found".format(city))


@app.route('/')
def index():
    return render_template("index.html")

# @app.route('/doc/')
# def swagger_ui():
#     return apidoc.ui_for(api)

@app.route("/<string:city>")
def city_view(city):
    check_city(city)
    return render_template('city.html', city=city)


@app.route("/<string:city>/<int:station_id>")
def station_view(city, station_id):
    check_city(city)
    return render_template('station.html', city=city, station_id=station_id)


if __name__ == '__main__':
    app.run(debug=True)
