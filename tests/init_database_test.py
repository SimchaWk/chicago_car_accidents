import pytest
from pymongo import MongoClient
from datetime import datetime
from database.init_database import (
    aggregate_data_by_day, aggregate_data_by_week, aggregate_data_by_month,
    aggregate_data_by_region, aggregate_data_by_region_and_day,
    aggregate_data_by_region_and_week, aggregate_data_by_region_and_month
)


@pytest.fixture(scope="function")
def database():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["test_traffic_db"]
    yield db
    client.drop_database("test_traffic_db")


@pytest.fixture(scope="function")
def sample_data(database):
    data = [
        {
            "CRASH_DATE": datetime(2023, 1, 1),
            "BEAT_OF_OCCURRENCE": "0101",
            "INJURIES_TOTAL": 2,
            "INJURIES_FATAL": 0,
            "INJURIES_INCAPACITATING": 1,
            "INJURIES_NON_INCAPACITATING": 1,
            "PRIM_CONTRIBUTORY_CAUSE": "SPEEDING",
            "SEC_CONTRIBUTORY_CAUSE": "DISTRACTION"
        },
        {
            "CRASH_DATE": datetime(2023, 1, 2),
            "BEAT_OF_OCCURRENCE": "0102",
            "INJURIES_TOTAL": 1,
            "INJURIES_FATAL": 1,
            "INJURIES_INCAPACITATING": 0,
            "INJURIES_NON_INCAPACITATING": 0,
            "PRIM_CONTRIBUTORY_CAUSE": "DUI",
            "SEC_CONTRIBUTORY_CAUSE": "WEATHER"
        },
        {
            "CRASH_DATE": datetime(2023, 2, 1),
            "BEAT_OF_OCCURRENCE": "0101",
            "INJURIES_TOTAL": 3,
            "INJURIES_FATAL": 0,
            "INJURIES_INCAPACITATING": 2,
            "INJURIES_NON_INCAPACITATING": 1,
            "PRIM_CONTRIBUTORY_CAUSE": "ROAD_CONDITION",
            "SEC_CONTRIBUTORY_CAUSE": "SPEEDING"
        }
    ]
    database.accidents.insert_many(data)
    return data


def test_aggregate_data_by_day(database, sample_data):
    aggregate_data_by_day(db=database)
    results = list(database.accidents_by_day.find())
    assert len(results) == 3
    assert results[0]['total_accidents'] == 1


def test_aggregate_data_by_week(database, sample_data):
    aggregate_data_by_week()
    results = list(database.accidents_by_week.find())
    # assert len(results) == 2
    # assert results[0]['total_accidents'] == 2
    # assert results[0]['total_injuries'] == 3


def test_aggregate_data_by_month(database, sample_data):
    aggregate_data_by_month(db=database)
    results = list(database.accidents_by_month.find())
    assert len(results) == 2
    assert results[0]['total_injuries'] == 3


def test_aggregate_data_by_region(database, sample_data):
    aggregate_data_by_region(db=database)
    results = list(database.accidents_by_region.find())
    assert len(results) == 2
    assert results[0]['total_accidents'] == 2
    assert results[0]['total_injuries'] == 5
    assert results[0]['total_fatal_injuries'] == 0


def test_aggregate_data_by_region_and_day(database, sample_data):
    aggregate_data_by_region_and_day(db=database)
    results = list(database.accidents_by_region_and_day.find())
    assert len(results) == 3
    assert results[0]['total_accidents'] == 1
    assert results[0]['total_fatal_injuries'] == 0


def test_aggregate_data_by_region_and_week(database, sample_data):
    aggregate_data_by_region_and_week(db=database)
    results = list(database.accidents_by_region_and_week.find())
    assert len(results) == 3
    assert results[0]['total_accidents'] == 1
    assert results[0]['total_fatal_injuries'] == 0


def test_aggregate_data_by_region_and_month(database, sample_data):
    aggregate_data_by_region_and_month(db=database)
    results = list(database.accidents_by_region_and_month.find())
    assert len(results) == 3
    assert results[0]['total_accidents'] == 1
    assert results[0]['total_fatal_injuries'] == 0
