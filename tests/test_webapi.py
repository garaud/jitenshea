import json
from datetime import date, datetime, timedelta

import pytest
from jitenshea.webapi import ISO_DATE, ISO_DATETIME, api
from jitenshea.webapp import app

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


def test_api_city_info_stations(client):
    resp = client.get('/api/bordeaux/infostation', query_string={'limit': 10})
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert 10 == len(data['data'])
    resp = client.get('/api/lyon/infostation', query_string={'limit': 5})
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert 5 == len(data['data'])
    assert ("address", "city", "id", "name", "nb_stands", "x", "y") == tuple(data['data'][0].keys())


def test_api_city_stations(client):
    resp = client.get('/api/bordeaux/station', query_string={'limit': 10})
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert 10 == len(data['data'])
    assert 'date' in data
    station = data['data'][0]
    assert ("id", "name", "nb_bikes", "nb_stands", "timestamp", "x", "y") == tuple(station.keys())
    resp = client.get('/api/lyon/station', query_string={'limit': 5})
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert 5 == len(data['data'])
    assert 'date' in data
    station = data['data'][0]
    assert ("id", "name", "nb_bikes", "nb_stands", "timestamp", "x", "y") == tuple(station.keys())


def test_api_city_map_stations(client):
    """Data in GeoJSON
    """
    resp = client.get('/api/bordeaux/station', query_string={'limit': 10,
                                                             'geojson': True})
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['type'] == 'FeatureCollection'
    assert 10 == len(data['features'])
    station = data['features'][0]
    assert station['geometry']['type'] == 'Point'
    assert tuple(station['properties'].keys()) == ("id", "name", "nb_bikes", "nb_stands", "timestamp")


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
    resp = client.get('/api/lyon/profile/hourly/station/1002',
                      query_string={"date": date})
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
    assert {'0', '1', '2', '3'} == set(x['cluster_id'] for x in data)
    resp = client.get('/api/bordeaux/clustering/stations',
                      query_string={"geojson": True})
    assert resp.status_code == 200


def test_api_clustering_centroids(client):
    resp = client.get('/api/bordeaux/clustering/centroids')
    assert resp.status_code == 200
    data = json.loads(resp.data)['data']
    assert {'0', '1', '2', '3'} == set(x['cluster_id'] for x in data)


def test_api_prediction(client):
    stop = datetime.today() + timedelta(hours=1)
    start = stop - timedelta(hours=2)
    params = {'start': start.strftime(ISO_DATETIME),
              'stop': stop.strftime(ISO_DATETIME)}
    print(params)
    resp = client.get('/api/bordeaux/predict/station/22',
                      query_string=params)
    assert resp.status_code == 200
    data = resp.get_json()
    # 3 values by default
    assert len(data) == 5
    assert 'nb_bikes' in data[0]
    assert data[0]['at'] == '1H'


def test_api_prediction_with_current_values(client):
    stop = datetime.today() + timedelta(hours=1)
    start = stop - timedelta(hours=2)
    params = {'start': start.strftime(ISO_DATETIME),
              'stop': stop.strftime(ISO_DATETIME),
              'current': True}
    print(params)
    resp = client.get('/api/bordeaux/predict/station/22',
                      query_string=params)
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) > 3
    assert 'nb_bikes' in data[0]


def test_api_latest_prediction(client):
    """Latest predictions for all stations.
    """
    resp = client.get('/api/bordeaux/predict/station')
    assert resp.status_code == 200
    data = resp.get_json()['data']
    date = resp.get_json()['date']
    assert len(data) >= 100
    # in GeoJSON
    resp = client.get('/api/bordeaux/predict/station',
                      query_string={'limit': 5, 'geojson': True})
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data['features']) == 5
    assert data['features'][0]['geometry']['type'] == 'Point'
