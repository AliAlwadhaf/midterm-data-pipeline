from pymongo import MongoClient


from config.settings import MONGO_URI, MONGO_DATABASE
DB_NAME = MONGO_DATABASE
COLLECTION_NAME = "orders_validated"


def get_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return client, db[COLLECTION_NAME]


def sales_by_city(limit=20):
    client, collection = get_collection()

    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
                "avg_order_value": {"$avg": "$total_amount"}
            }
        },
        {
            "$sort": {
                "total_sales": -1
            }
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "order_count": 1,
                "total_sales": 1,
                "avg_order_value": 1
            }
        }
    ]

    result = list(collection.aggregate(pipeline))
    client.close()
    return result


def sales_by_customer(limit=20):
    client, collection = get_collection()

    pipeline = [
        {
            "$group": {
                "_id": {
                    "customer_id": "$customer_id",
                    "customer_name": "$customer_name"
                },
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"}
            }
        },
        {
            "$sort": {
                "total_sales": -1
            }
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id.customer_id",
                "customer_name": "$_id.customer_name",
                "order_count": 1,
                "total_sales": 1
            }
        }
    ]

    result = list(collection.aggregate(pipeline))
    client.close()
    return result


def sales_by_payment_status():
    client, collection = get_collection()

    pipeline = [
        {
            "$group": {
                "_id": "$payment_status",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
                "avg_order_value": {"$avg": "$total_amount"}
            }
        },
        {
            "$sort": {
                "total_sales": -1
            }
        },
        {
            "$project": {
                "_id": 0,
                "payment_status": "$_id",
                "order_count": 1,
                "total_sales": 1,
                "avg_order_value": 1
            }
        }
    ]

    result = list(collection.aggregate(pipeline))
    client.close()
    return result


def orders_by_status():
    client, collection = get_collection()

    pipeline = [
        {
            "$group": {
                "_id": "$status",
                "order_count": {"$sum": 1,
                },
                "total_sales": {"$sum": "$total_amount"}
            }
        },
        {
            "$sort": {
                "order_count": -1
            }
        },
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "order_count": 1,
                "total_sales": 1
            }
        }
    ]

    result = list(collection.aggregate(pipeline))
    client.close()
    return result


def daily_sales(limit=30):
    client, collection = get_collection()

    pipeline = [
        {
            "$set": {
                "_order_date": {
                    "$dateFromString": {
                        "dateString": "$order_date",
                        "onError": None,
                        "onNull": None
                    }
                }
            }
        },
        {
            "$match": {
                "_order_date": {
                    "$ne": None
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$_order_date"
                    }
                },
                "order_count": {"$sum": 1,},
                "total_sales": {"$sum": "$total_amount"}
            }
        },
        {
            "$sort": {
                "_id": -1
            }
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "order_count": 1,
                "total_sales": 1
            }
        }
    ]

    result = list(collection.aggregate(pipeline))
    client.close()
    return result


AGGREGATIONS = {
    "sales_by_city": sales_by_city,
    "sales_by_customer": sales_by_customer,
    "sales_by_payment_status": sales_by_payment_status,
    "orders_by_status": orders_by_status,
    "daily_sales": daily_sales,
}


if __name__ == "__main__":
    print("\nAvailable aggregations:")
    for name in AGGREGATIONS:
        print("-", name)

    print("\nTesting sales_by_city:")
    for row in sales_by_city(5):
        print(row)

