import pytest
from flask import Flask

from controllers.accidents_controller import accidents_bp


@pytest.fixture
def client():
    app = Flask(__name__)
    app.register_blueprint(accidents_bp)
    with app.test_client() as client:
        yield client


def test_get_total_accidents(client):
    response = client.get('/total_accidents/512')
    assert response.status_code == 200
    data = response.get_json()
    assert 'total_accidents' in data


def test_get_accidents_by_period(client):
    response = client.get('/accidents_by_period?region=512&period_type=day&date=2023-05-15')
    assert response.status_code in [200, 404]
    data = response.get_json()
    assert 'total_accidents' in data


def test_get_accidents_by_cause(client):
    response = client.get('/accidents_by_cause?region=512')
    assert response.status_code == 200
    data = response.get_json()
    assert 'accident_causes' in data


def test_get_injury_statistics(client):
    response = client.get('/injury_statistics?region=512')
    assert response.status_code == 200
    data = response.get_json()
    assert 'total_injuries' in data
    assert 'fatal_injuries' in data
    assert 'non_fatal_injuries' in data


def test_invalid_region(client):
    response = client.get('/total_accidents/invalid')
    assert response.status_code == 400
    data = response.get_json()
    assert 'error' in data


def test_missing_parameters(client):
    response = client.get('/accidents_by_period')
    assert response.status_code == 400
    data = response.get_json()
    assert 'error' in data
