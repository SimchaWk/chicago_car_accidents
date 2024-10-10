from repository.accidents_repository import *


def test_get_total_accidents_by_region():
    region_code = 512
    result = get_total_accidents_by_region(region_code)
    assert isinstance(result, Success)
    data = result.unwrap()
    assert 'total_accidents' in data
    assert isinstance(data['total_accidents'], int)
    assert data['total_accidents'] >= 0


def test_get_accidents_by_region_and_period():
    region = "512"
    period_type = "day"
    date = datetime(2023, 5, 15)
    result = get_accidents_by_region_and_period(region, period_type, date)
    assert isinstance(result, Success)
    data = result.unwrap()
    assert 'total_accidents' in data
    assert isinstance(data['total_accidents'], int)
    assert data['total_accidents'] >= 0


def test_get_accidents_by_primary_cause():
    region = 512
    result = get_accidents_by_primary_cause(region)
    assert isinstance(result, Success)
    data = result.unwrap()
    assert 'accident_causes' in data
    assert isinstance(data['accident_causes'], list)
    if data['accident_causes']:
        assert 'primary_cause' in data['accident_causes'][0]
        assert 'accident_count' in data['accident_causes'][0]


def test_get_injury_statistics():
    region = 512
    result = get_injury_statistics(region)
    assert isinstance(result, Success)
    data = result.unwrap()
    assert 'total_injuries' in data
    assert 'fatal_injuries' in data
    assert 'non_fatal_injuries' in data
