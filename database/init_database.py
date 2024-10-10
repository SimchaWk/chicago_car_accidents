import pandas as pd
from pymongo import MongoClient

from database.connect import get_database

ACCIDENTS_DATA_PATH = 'C:/Users/Simch/PycharmProjects/chicago_car_accidents/data/Traffic_Crashes_Crashes.csv'
traffic_db = get_database()


def parse_date(date_str: str):
    try:
        return pd.to_datetime(date_str, format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
    except ValueError:
        return pd.to_datetime(date_str, format='%m/%d/%Y %H:%M', errors='coerce')


def load_data_to_mongo(csv_file_path: str, db = traffic_db):
    db = db

    df = pd.read_csv(csv_file_path, dtype={'CRASH_DATE': str})
    df['CRASH_DATE'] = df['CRASH_DATE'].apply(parse_date)
    accidents = df[['CRASH_DATE', 'BEAT_OF_OCCURRENCE', 'INJURIES_TOTAL', 'INJURIES_FATAL',
                    'INJURIES_INCAPACITATING', 'INJURIES_NON_INCAPACITATING',
                    'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE']].copy()
    db['accidents'].insert_many(accidents.to_dict('records'))
    db['accidents'].create_index("CRASH_DATE")
    db['accidents'].create_index("BEAT_OF_OCCURRENCE")


def aggregate_data_by_day(db = traffic_db):
    db = db
    accidents = db['accidents']
    pipeline = [
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$CRASH_DATE"}},
            "total_accidents": {"$sum": 1},
            "total_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_TOTAL", None]},
                                                            {"$ne": ["$INJURIES_TOTAL", float('nan')]}]},
                                                  "$INJURIES_TOTAL", 0]}},
            "total_fatal_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_FATAL", None]},
                                                                  {"$ne": ["$INJURIES_FATAL", float('nan')]}]},
                                                        "$INJURIES_FATAL", 0]}},
            "incapacitating_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_INCAPACITATING", None]},
                                                                     {"$ne": ["$INJURIES_INCAPACITATING",
                                                                              float('nan')]}]},
                                                           "$INJURIES_INCAPACITATING", 0]}}
        }},
        {"$project": {
            "date": "$_id", "total_accidents": 1, "total_injuries": 1,
            "total_fatal_injuries": 1, "incapacitating_injuries": 1, "_id": 0
        }}
    ]
    result = list(accidents.aggregate(pipeline))
    if result:
        db['accidents_by_day'].insert_many(result)
    db['accidents_by_day'].create_index("date")
    print('aggregate_data_by_day finished successfully!')


def aggregate_data_by_week(db = traffic_db):
    db = db
    accidents = db['accidents']
    pipeline = [
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%U", "date": "$CRASH_DATE"}},
            "total_accidents": {"$sum": 1},
            "total_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_TOTAL", None]},
                                                            {"$ne": ["$INJURIES_TOTAL", float('nan')]}]},
                                                  "$INJURIES_TOTAL", 0]}},
            "total_fatal_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_FATAL", None]},
                                                                  {"$ne": ["$INJURIES_FATAL", float('nan')]}]},
                                                        "$INJURIES_FATAL", 0]}},
            "incapacitating_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_INCAPACITATING", None]},
                                                                     {"$ne": ["$INJURIES_INCAPACITATING",
                                                                              float('nan')]}]},
                                                           "$INJURIES_INCAPACITATING", 0]}}
        }},
        {"$project": {
            "year_week": "$_id", "total_accidents": 1, "total_injuries": 1,
            "total_fatal_injuries": 1, "incapacitating_injuries": 1, "_id": 0
        }}
    ]
    result = list(accidents.aggregate(pipeline))
    if result:
        db['accidents_by_week'].insert_many(result)
    db['accidents_by_week'].create_index("year_week")
    print('aggregate_data_by_week finished successfully!')


def aggregate_data_by_month(db = traffic_db):
    db = db
    accidents = db['accidents']
    pipeline = [
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m", "date": "$CRASH_DATE"}},
            "total_accidents": {"$sum": 1},
            "total_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_TOTAL", None]},
                                                            {"$ne": ["$INJURIES_TOTAL", float('nan')]}]},
                                                  "$INJURIES_TOTAL", 0]}},
            "total_fatal_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_FATAL", None]},
                                                                  {"$ne": ["$INJURIES_FATAL", float('nan')]}]},
                                                        "$INJURIES_FATAL", 0]}},
            "incapacitating_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_INCAPACITATING", None]},
                                                                     {"$ne": ["$INJURIES_INCAPACITATING",
                                                                              float('nan')]}]},
                                                           "$INJURIES_INCAPACITATING", 0]}}
        }},
        {"$project": {
            "year_month": "$_id", "total_accidents": 1, "total_injuries": 1,
            "total_fatal_injuries": 1, "incapacitating_injuries": 1, "_id": 0
        }}
    ]
    result = list(accidents.aggregate(pipeline))
    if result:
        db['accidents_by_month'].insert_many(result)
    db['accidents_by_month'].create_index("year_month")
    print('aggregate_data_by_month finished successfully!')


def aggregate_data_by_region(db = traffic_db):
    db = db
    accidents = db['accidents']
    pipeline = [
        {"$group": {
            "_id": "$BEAT_OF_OCCURRENCE",
            "total_accidents": {"$sum": 1},
            "total_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_TOTAL", None]},
                                                            {"$ne": ["$INJURIES_TOTAL", float('nan')]}]},
                                                  "$INJURIES_TOTAL", 0]}},
            "total_fatal_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_FATAL", None]},
                                                                  {"$ne": ["$INJURIES_FATAL", float('nan')]}]},
                                                        "$INJURIES_FATAL", 0]}},
            "incapacitating_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_INCAPACITATING", None]},
                                                                     {"$ne": ["$INJURIES_INCAPACITATING",
                                                                              float('nan')]}]},
                                                           "$INJURIES_INCAPACITATING", 0]}}
        }},
        {"$project": {
            "region": "$_id", "total_accidents": 1, "total_injuries": 1,
            "total_fatal_injuries": 1, "incapacitating_injuries": 1, "_id": 0
        }}
    ]
    result = list(accidents.aggregate(pipeline))
    if result:
        db['accidents_by_region'].insert_many(result)
    db['accidents_by_region'].create_index("region")
    print('aggregate_data_by_region finished successfully!')


def aggregate_data_by_region_and_day(db = traffic_db):
    db = db
    accidents = db['accidents']
    pipeline = [
        {"$group": {
            "_id": {
                "region": "$BEAT_OF_OCCURRENCE",
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$CRASH_DATE"}}
            },
            "total_accidents": {"$sum": 1},
            "total_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_TOTAL", None]},
                                                            {"$ne": ["$INJURIES_TOTAL", float('nan')]}]},
                                                  "$INJURIES_TOTAL", 0]}},
            "total_fatal_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_FATAL", None]},
                                                                  {"$ne": ["$INJURIES_FATAL", float('nan')]}]},
                                                        "$INJURIES_FATAL", 0]}},
            "incapacitating_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_INCAPACITATING", None]},
                                                                     {"$ne": ["$INJURIES_INCAPACITATING",
                                                                              float('nan')]}]},
                                                           "$INJURIES_INCAPACITATING", 0]}}
        }},
        {"$project": {
            "region": "$_id.region", "date": "$_id.date", "total_accidents": 1,
            "total_injuries": 1, "total_fatal_injuries": 1, "incapacitating_injuries": 1, "_id": 0
        }}
    ]
    result = list(accidents.aggregate(pipeline))
    if result:
        db['accidents_by_region_and_day'].insert_many(result)
    db['accidents_by_region_and_day'].create_index([("region", 1), ("date", 1)])
    print('aggregate_data_by_region_and_day finished successfully!')


def aggregate_data_by_region_and_week(db = traffic_db):
    db = db
    accidents = db['accidents']
    pipeline = [
        {"$group": {
            "_id": {
                "region": "$BEAT_OF_OCCURRENCE",
                "year_week": {"$dateToString": {"format": "%Y-%U", "date": "$CRASH_DATE"}}
            },
            "total_accidents": {"$sum": 1},
            "total_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_TOTAL", None]},
                                                            {"$ne": ["$INJURIES_TOTAL", float('nan')]}]},
                                                  "$INJURIES_TOTAL", 0]}},
            "total_fatal_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_FATAL", None]},
                                                                  {"$ne": ["$INJURIES_FATAL", float('nan')]}]},
                                                        "$INJURIES_FATAL", 0]}},
            "incapacitating_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_INCAPACITATING", None]},
                                                                     {"$ne": ["$INJURIES_INCAPACITATING",
                                                                              float('nan')]}]},
                                                           "$INJURIES_INCAPACITATING", 0]}}
        }},
        {"$project": {
            "region": "$_id.region", "year_week": "$_id.year_week", "total_accidents": 1,
            "total_injuries": 1, "total_fatal_injuries": 1, "incapacitating_injuries": 1, "_id": 0
        }}
    ]
    result = list(accidents.aggregate(pipeline))
    if result:
        db['accidents_by_region_and_week'].insert_many(result)
    db['accidents_by_region_and_week'].create_index([("region", 1), ("year_week", 1)])
    print('aggregate_data_by_region_and_week finished successfully!')


def aggregate_data_by_region_and_month(db = traffic_db):
    db = db
    accidents = db['accidents']
    pipeline = [
        {"$group": {
            "_id": {
                "region": "$BEAT_OF_OCCURRENCE",
                "year_month": {"$dateToString": {"format": "%Y-%m", "date": "$CRASH_DATE"}}
            },
            "total_accidents": {"$sum": 1},
            "total_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_TOTAL", None]},
                                                            {"$ne": ["$INJURIES_TOTAL", float('nan')]}]},
                                                  "$INJURIES_TOTAL", 0]}},
            "total_fatal_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_FATAL", None]},
                                                                  {"$ne": ["$INJURIES_FATAL", float('nan')]}]},
                                                        "$INJURIES_FATAL", 0]}},
            "incapacitating_injuries": {"$sum": {"$cond": [{"$and": [{"$ne": ["$INJURIES_INCAPACITATING", None]},
                                                                     {"$ne": ["$INJURIES_INCAPACITATING",
                                                                              float('nan')]}]},
                                                           "$INJURIES_INCAPACITATING", 0]}}
        }},
        {"$project": {
            "region": "$_id.region", "year_month": "$_id.year_month", "total_accidents": 1,
            "total_injuries": 1, "total_fatal_injuries": 1, "incapacitating_injuries": 1, "_id": 0
        }}
    ]
    result = list(accidents.aggregate(pipeline))
    if result:
        db['accidents_by_region_and_month'].insert_many(result)
    db['accidents_by_region_and_month'].create_index([("region", 1), ("year_month", 1)])
    print('aggregate_data_by_region_and_month finished successfully!')


if __name__ == "__main__":
    # load_data_to_mongo(ACCIDENTS_DATA_PATH)
    # aggregate_data_by_day()
    # aggregate_data_by_week()
    # aggregate_data_by_month()
    # aggregate_data_by_region()
    # aggregate_data_by_region_and_day()
    # aggregate_data_by_region_and_week()
    # aggregate_data_by_region_and_month()
    print()
