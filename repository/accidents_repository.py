from returns.result import Result, Success, Failure
from typing import Dict, List
from database.connect import *
from database.init_database import *

from returns.result import Result, Success, Failure
from datetime import datetime
from typing import Dict


def init_db_with_data():
    drop_database()
    load_data_to_mongo(ACCIDENTS_DATA_PATH)
    aggregate_data_by_day()
    aggregate_data_by_week()
    aggregate_data_by_month()
    aggregate_data_by_region()
    aggregate_data_by_region_and_day()
    aggregate_data_by_region_and_week()
    aggregate_data_by_region_and_month()


def get_total_accidents_by_region(region_code: int, collection=accidents_by_region) -> Result[Dict, str]:
    try:
        result = collection.find_one({"region": region_code})
        return Success(result)
    except Exception as e:
        return Failure(f"Failed to get total accidents for region {region_code}: {str(e)}")


def get_accidents_by_region_and_period(region: int, period_type: str, date: datetime) -> Result[Dict, str]:
    try:
        db = get_database()

        if period_type == 'day':
            collection = db.accidents_by_region_and_day
            date_field = "date"
            formatted_date = date.strftime("%Y-%m-%d")
        elif period_type == 'week':
            collection = db.accidents_by_region_and_week
            date_field = "year_week"
            formatted_date = date.strftime("%Y-%W")
        elif period_type == 'month':
            collection = db.accidents_by_region_and_month
            date_field = "year_month"
            formatted_date = date.strftime("%Y-%m")
        else:
            return Failure(f"Invalid period type: {period_type}")

        query = {
            "region": region,
            date_field: formatted_date
        }

        result = collection.find_one(query)

        if result:
            return Success({
                "region": region,
                "period_type": period_type,
                "date": formatted_date,
                "total_accidents": result.get('total_accidents', 0)
            })
        else:
            return Success({
                "region": region,
                "period_type": period_type,
                "date": formatted_date,
                "total_accidents": 0,
                "message": "No data found for this period and region"
            })
    except Exception as e:
        return Failure(f"Failed to get accidents for region {region} and period {period_type}: {str(e)}")


def get_accidents_by_primary_cause(region: int) -> Result[List[Dict], str]:
    try:
        db = get_database()

        pipeline = [
            {"$match": {"BEAT_OF_OCCURRENCE": region}},
            {"$group": {
                "_id": "$PRIM_CONTRIBUTORY_CAUSE",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$project": {
                "_id": 0,
                "primary_cause": "$_id",
                "accident_count": "$count"
            }}
        ]

        results = list(db.accidents.aggregate(pipeline))

        if results:
            return Success({
                "region": region,
                "accident_causes": results
            })
        else:
            return Success({
                "region": region,
                "accident_causes": [],
                "message": "No data found for this region"
            })
    except Exception as e:
        return Failure(f"Failed to get accidents by primary cause for region {region}: {str(e)}")


def get_injury_statistics(region: int) -> Result[Dict, str]:
    try:
        db = get_database()

        pipeline = [
            {"$match": {"BEAT_OF_OCCURRENCE": region}},
            {"$group": {
                "_id": None,
                "total_injuries": {"$sum": "$INJURIES_TOTAL"},
                "fatal_injuries": {"$sum": "$INJURIES_FATAL"},
                "non_fatal_injuries": {"$sum": {"$subtract": ["$INJURIES_TOTAL", "$INJURIES_FATAL"]}},
                "fatal_events": {
                    "$push": {
                        "$cond": [
                            {"$gt": ["$INJURIES_FATAL", 0]},
                            {
                                "date": "$CRASH_DATE",
                                "fatal_injuries": "$INJURIES_FATAL",
                                "total_injuries": "$INJURIES_TOTAL"
                            },
                            "$$REMOVE"
                        ]
                    }
                },
                "non_fatal_events": {
                    "$push": {
                        "$cond": [
                            {"$and": [
                                {"$gt": ["$INJURIES_TOTAL", 0]},
                                {"$eq": ["$INJURIES_FATAL", 0]}
                            ]},
                            {
                                "date": "$CRASH_DATE",
                                "injuries": "$INJURIES_TOTAL"
                            },
                            "$$REMOVE"
                        ]
                    }
                }
            }},
            {"$project": {
                "_id": 0,
                "total_injuries": 1,
                "fatal_injuries": 1,
                "non_fatal_injuries": 1,
                "fatal_events": 1,
                "non_fatal_events": 1
            }}
        ]

        result = list(db.accidents.aggregate(pipeline))

        if result:
            stats = result[0]
            stats["region"] = region
            return Success(stats)
        else:
            return Success({
                "region": region,
                "total_injuries": 0,
                "fatal_injuries": 0,
                "non_fatal_injuries": 0,
                "fatal_events": [],
                "non_fatal_events": [],
                "message": "No injury data found for this region"
            })
    except Exception as e:
        return Failure(f"Failed to get injury statistics for region {region}: {str(e)}")


if __name__ == '__main__':
    print(get_total_accidents_by_region(1235))
    print(get_accidents_by_region_and_period(1235, 'week', datetime(2023, 5, 12)))
    print(get_accidents_by_primary_cause(1235))
    print(get_injury_statistics(512))
