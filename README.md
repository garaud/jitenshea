# Jitenshea: bicycle-sharing data analysis

 [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

In Japanese:

Jitensha (bicycle) + Shea (share) = Jitenshea

Analyze bikes sharing station data some cities where there are Open Data.

You have two parts in this project:

* a data pipeline and data processing with [luigi](http://luigi.readthedocs.io/)
  to get, transform and store data

* Web application to get and visualize some data through a REST API

![screenshot](./webapp-screenshot.png)

## Data

Open Data from French cities Bordeaux and Lyon:

* [Data Grand Lyon Website](https://data.grandlyon.com/equipements/station-vflov-disponibilitfs-temps-rfel/)
* [Data Bordeaux Website](https://data.bordeaux-metropole.fr/data.php?themes=10)

Some luigi [tasks](./jitenshea/tasks) can be called every 10 minutes for instance
to gather the bicycle-sharing stations data. Another one is called every day to
aggregate some data. You can use [cron-job](https://cron-job.org/en/) to carry
out this stuff.

## Configuration

A configuration file sample can be found at the root directory
[config.ini.sample](./config.ini.sample). Copy it into the `jitenshea` directory
, rename it into `config.ini` and update it.

It is used for the database access, some tokens for API, etc.

In the case of Bordeaux data, a key is needed to get the data on the open data
portal. The only step for obtaining such a key is to subscribe to
the [portal](https://data.bordeaux-metropole.fr/key).

If you want to gather weather data, a subscription
to [Openweather API](https://home.openweathermap.org/users/sign_in) is
required.

For the wep application, you can launch `bower` to install Javascript and CSS dependencies.

## Requirements

PostgreSQL database with PostGIS. You must have the `shp2pgsql` command.

* Python 3.6
* pandas
* requests
* luigi
* sh
* psycopg2
* sqlalchemy
* flask-restplus
* daiquiri

See the `conda_env.sh` script to create a conda environment with the dependencies.

**Note**: flask-restplus and daiquiri should be install via `pip`.

## Tests

Install the extras dependencies, e.g. `pip install -e ."[dev]"`, and run `pytest`.
