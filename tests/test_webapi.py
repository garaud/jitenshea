import json
from datetime import date, timedelta

import pytest

from jitenshea.webapp import app
from jitenshea.webapi import api, ISO_DATE


app.config['TESTING'] = True
api.init_app(app)


def yesterday():
    return date.today() - timedelta(1)


@pytest.fixture
def client():
    client = app.test_client()
    return client


def test_app_index(client):
    resp = client.get('/')
    assert resp.status_code == 200


def test_api_city_list(client):
    resp = client.get('/api/city')
    assert resp.status_code == 200
    content = json.loads(resp.data)
    expected = [{"city": "lyon",
                 "country": "france",
                 "stations": 348},
                {"city": "bordeaux",
                 "country": "france",
                 "stations": 174}]
    assert expected == content['data']


def test_api_city_stations(client):
    resp = client.get('/api/bordeaux/station', query_string={'limit': 10})
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert 10 == len(data['data'])
    resp = client.get('/api/lyon/station', query_string={'limit': 5})
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert 5 == len(data['data'])


def test_api_specific_stations(client):
    resp = client.get('/api/bordeaux/station/93,35')
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert len(data['data']) == 2
    assert ['35', '93'] == [x['id'] for x in data['data']]


def test_api_daily_transaction(client):
    date = yesterday().strftime(ISO_DATE)
    resp = client.get('/api/bordeaux/daily/station',
                      query_string={"limit": 10, "date": date, "by": "value"})
    assert resp.status_code == 200
    data = json.loads(resp.data)['data']
    # order by value must return the first station transaction value higher than the
    # second one.
    assert data[0]['value'][0] > data[1]['value'][0]


def test_api_timeseries(client):
    start = yesterday().strftime(ISO_DATE)
    stop = date.today().strftime(ISO_DATE)
    resp = client.get('/api/bordeaux/timeseries/station/93,33',
                      query_string={"start": start, "stop": stop})
    assert resp.status_code == 200


def test_api_hourly_profile(client):
    date = yesterday().strftime(ISO_DATE)
    resp = client.get('/api/bordeaux/profile/hourly/station/93,33',
                      query_string={'date': date,
                                    'window': 2})
    assert resp.status_code == 200


def test_api_daily_profile(client):
    date = yesterday().strftime(ISO_DATE)
    resp = client.get('/api/bordeaux/profile/daily/station/93,33',
                      query_string={"date": date})
    assert resp.status_code == 200


def test_api_clustering_stations(client):
    resp = client.get('/api/bordeaux/clustering/stations')
    assert resp.status_code == 200
    data = json.loads(resp.data)['data']
    # there are just 4 clusters
    assert {0, 1, 2, 3} == set(x['cluster_id'] for x in data)


def test_api_clustering_centroids(client):
    resp = client.get('/api/bordeaux/clustering/centroids')
    assert resp.status_code == 200
    data = json.loads(resp.data)['data']
    assert {0, 1, 2, 3} == set(x['cluster_id'] for x in data)
