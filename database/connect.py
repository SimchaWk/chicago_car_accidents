from pymongo import MongoClient

def get_database():
    client = MongoClient("mongodb://localhost:27017/")
    return client["traffic_db"]


def drop_database():
    db = get_database()

    db['accidents'].drop()
    db['accidents_by_day'].drop()
    db['accidents_by_week'].drop()
    db['accidents_by_month'].drop()
    db['accidents_by_region'].drop()
    db['accidents_by_region_and_day'].drop()
    db['accidents_by_region_and_week'].drop()
    db['accidents_by_region_and_month'].drop()

db = get_database()
accidents = db['accidents']
accidents_by_day = db['accidents_by_day']
accidents_by_week = db['accidents_by_week']
accidents_by_month = db['accidents_by_month']
accidents_by_region = db['accidents_by_region']
accidents_by_region_and_day = db['accidents_by_region_and_day']
accidents_by_region_and_week = db['accidents_by_region_and_week']
accidents_by_region_and_month = db['accidents_by_region_and_month']
