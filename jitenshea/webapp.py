# coding: utf-8

"""Flask Web Application for Jitenshea (Bicycle-sharing data)
"""

import daiquiri

from flask import Flask, render_template, abort


logger = daiquiri.getLogger("jitenshea-webapp")

app = Flask(__name__)
app.config['ERROR_404_HELP'] = False
app.config['SWAGGER_UI_DOC_EXPANSION'] = 'list'

CITIES = ['bordeaux', 'lyon']
CITIES_DESC = [{
                    'id': 'bordeaux',
                    'title': 'Bordeaux', 
                    'img': 'img/bordeaux.jpg',
                    'desc': 'The famous French city for its wine. Visit the Website dedicated to their',
                    'opendata_url': 'http://opendata.bordeaux.fr/recherche/results',
                    'stats': 'More than 170 bicycle stations, +10,000 transactions in a day'
                },
               {
                    'id': 'lyon',
                    'title': 'Lyon',
                    'img': 'img/lyon.jpg',
                    'desc': 'Lyon is the one of the largest cities of France. You can find the Website dedicated to their ',
                    'opendata_url': 'https://data.grandlyon.com/',
                    'stats': 'More than 340 bicycle stations, +30,000 transactions in a day'
                }]

def check_city(city):
    if city not in CITIES:
        abort(404, "City {} not found".format(city))

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/doc/')
def swagger_ui():
    return render_template("swagger-ui.html")

@app.route('/city')
def citylist():
    cities = CITIES_DESC
    return render_template('citylist.html', cities=cities)

@app.route("/<string:city>")
def city_view(city):
    check_city(city)
    return render_template('city.html', city=city)


@app.route("/<string:city>/<int:station_id>")
def station_view(city, station_id):
    check_city(city)
    return render_template('station.html', city=city, station_id=station_id)


@app.route("/<string:city>/cluster")
def clustering_view(city):
    check_city(city)
    return render_template("cluster.html", city=city)
