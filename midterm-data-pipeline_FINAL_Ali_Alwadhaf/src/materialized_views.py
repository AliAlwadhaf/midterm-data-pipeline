import json
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne


MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "midterm_orders"

ORDERS_COLLECTION = "orders_validated"
DAILY_COLLECTION = "daily_sales_summary"
PRODUCTS_COLLECTION = "top_products_summary"
STATE_COLLECTION = "materialized_view_state"

BATCH_SIZE = 5000


def get_db():
    client = MongoClient(MONGO_URI)
    return client, client[DB_NAME]


def _parse_items(items_json):
    if not items_json:
        return []

    try:
        items = json.loads(items_json)

        if not isinstance(items, list):
            return []

        return [
            item
            for item in items
            if isinstance(item, dict) and item.get("sku")
        ]

    except (json.JSONDecodeError, TypeError):
        return []


def refresh_daily_sales_incremental(db, orders):
    state = db[STATE_COLLECTION]

    state_doc = state.find_one({
        "_id": "daily_sales_summary"
    })

    last_source_row = (
        int(state_doc.get("last_processed_source_row", 0))
        if state_doc
        else 0
    )

    cursor = orders.find(
        {
            "source_row_number": {
                "$gt": last_source_row
            }
        },
        {
            "order_date": 1,
            "total_amount": 1,
            "source_row_number": 1
        }
    ).sort("source_row_number", 1).batch_size(BATCH_SIZE)

    operations = []
    max_source_row = last_source_row
    processed = 0

    for doc in cursor:

        try:
            source_row = int(doc["source_row_number"])
            date = doc["order_date"][:10]
            total = float(doc.get("total_amount") or 0)

            operations.append(
                UpdateOne(
                    {"date": date},
                    {
                        "$inc": {
                            "order_count": 1,
                            "total_sales": total
                        },
                        "$set": {
                            "last_updated": datetime.now(timezone.utc)
                        }
                    },
                    upsert=True
                )
            )

            max_source_row = max(
                max_source_row,
                source_row
            )

            processed += 1

            if len(operations) >= BATCH_SIZE:
                db[DAILY_COLLECTION].bulk_write(
                    operations,
                    ordered=False
                )

                operations.clear()

                if processed % 50000 == 0:
                    print(
                        f"Daily sales processed: {processed:,}"
                    )

        except (ValueError, TypeError, AttributeError):
            continue

    if operations:
        db[DAILY_COLLECTION].bulk_write(
            operations,
            ordered=False
        )

    state.update_one(
        {"_id": "daily_sales_summary"},
        {
            "$set": {
                "last_processed_source_row": max_source_row,
                "last_updated": datetime.now(timezone.utc)
            }
        },
        upsert=True
    )

    return {
        "processed": processed,
        "processed_from": last_source_row,
        "processed_to": max_source_row
    }


def refresh_top_products_incremental(db, orders):
    state = db[STATE_COLLECTION]

    state_doc = state.find_one({
        "_id": "top_products_summary"
    })

    last_source_row = (
        int(state_doc.get("last_processed_source_row", 0))
        if state_doc
        else 0
    )

    cursor = orders.find(
        {
            "source_row_number": {
                "$gt": last_source_row
            }
        },
        {
            "items_json": 1,
            "source_row_number": 1
        }
    ).sort("source_row_number", 1).batch_size(BATCH_SIZE)

    product_totals = {}
    max_source_row = last_source_row
    processed = 0

    def flush_products():

        if not product_totals:
            return

        operations = []

        for sku, values in product_totals.items():

            operations.append(
                UpdateOne(
                    {"sku": sku},
                    {
                        "$set": {
                            "name": values["name"],
                            "last_updated": datetime.now(
                                timezone.utc
                            )
                        },
                        "$inc": {
                            "quantity": values["quantity"],
                            "total_sales": values["total_sales"]
                        }
                    },
                    upsert=True
                )
            )

        db[PRODUCTS_COLLECTION].bulk_write(
            operations,
            ordered=False
        )

        product_totals.clear()

    for doc in cursor:

        try:
            source_row = int(doc["source_row_number"])

            items = _parse_items(
                doc.get("items_json")
            )

            for item in items:

                sku = str(
                    item.get("sku", "")
                ).strip()

                if not sku:
                    continue

                name = str(
                    item.get("name", "")
                ).strip()

                qty = float(
                    item.get("qty") or 0
                )

                total = float(
                    item.get("total") or 0
                )

                if sku not in product_totals:
                    product_totals[sku] = {
                        "name": name,
                        "quantity": 0,
                        "total_sales": 0
                    }

                product_totals[sku]["quantity"] += qty
                product_totals[sku]["total_sales"] += total

            max_source_row = max(
                max_source_row,
                source_row
            )

            processed += 1

            if processed % BATCH_SIZE == 0:

                flush_products()

                if processed % 50000 == 0:
                    print(
                        f"Top products processed: "
                        f"{processed:,}"
                    )

        except (ValueError, TypeError):
            continue

    flush_products()

    state.update_one(
        {"_id": "top_products_summary"},
        {
            "$set": {
                "last_processed_source_row": max_source_row,
                "last_updated": datetime.now(timezone.utc)
            }
        },
        upsert=True
    )

    return {
        "processed": processed,
        "processed_from": last_source_row,
        "processed_to": max_source_row
    }


def refresh_materialized_views():

    client, db = get_db()

    try:

        orders = db[ORDERS_COLLECTION]

        print("Refreshing daily_sales_summary...")

        daily_result = refresh_daily_sales_incremental(
            db,
            orders
        )

        print("\nRefreshing top_products_summary...")

        products_result = refresh_top_products_incremental(
            db,
            orders
        )

        return {
            "status": "success",
            "daily_sales_summary": daily_result,
            "top_products_summary": products_result
        }

    finally:
        client.close()


def show_materialized_views(limit=10):

    client, db = get_db()

    try:

        print("\nDAILY SALES SUMMARY")
        print("=" * 60)

        for doc in db[DAILY_COLLECTION].find(
            {},
            {"_id": 0}
        ).sort(
            "date",
            -1
        ).limit(limit):

            print(doc)

        print("\nTOP PRODUCTS SUMMARY")
        print("=" * 60)

        for doc in db[PRODUCTS_COLLECTION].find(
            {},
            {"_id": 0}
        ).sort(
            "total_sales",
            -1
        ).limit(limit):

            print(doc)

    finally:
        client.close()


if __name__ == "__main__":

    print("Refreshing materialized views...")

    result = refresh_materialized_views()

    print("\nRESULT:")
    print(result)

    show_materialized_views()