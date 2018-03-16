# coding: utf-8

"""Flask Web Application for Jitenshea (Bicycle-sharing data)
"""

import daiquiri
import logging

from flask import Flask, render_template, abort


daiquiri.setup(level=logging.INFO)
logger = daiquiri.getLogger("jitenshea-webapp")


app = Flask(__name__)
app.config['ERROR_404_HELP'] = False
app.config['SWAGGER_UI_DOC_EXPANSION'] = 'list'

CITIES = ['bordeaux', 'lyon']


def check_city(city):
    if city not in CITIES:
        abort(404, "City {} not found".format(city))

@app.route('/')
def index():
    """Render the index html webpage
    """
    return render_template("index.html")

@app.route('/doc/')
def swagger_ui():
    return render_template("swagger-ui.html")

@app.route("/<string:city>")
def city_view(city):
    """Render the city html webpage

    Parameters
    ----------
    city : str
        City that must be rendered on the web browser, either `Lyon` or `Bordeaux`
    """
    check_city(city)
    return render_template('city.html', city=city)


@app.route("/<string:city>/<int:station_id>")
def station_view(city, station_id):
    """Render the station html webpage, for given `station_id` in `city`

    Parameters
    ----------
    city : str
        City of interest, either `Lyon` or `Bordeaux`
    station_id : int
        ID of the station that must be rendered on the web browser
    """
    check_city(city)
    return render_template('station.html', city=city, station_id=station_id)


@app.route("/<string:city>/cluster")
def clustering_view(city):
    check_city(city)
    return render_template("cluster.html", city=city)
