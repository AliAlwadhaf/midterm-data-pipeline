from pymongo import MongoClient, ASCENDING


VALIDATED_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "order_id",
            "customer_id",
            "quality_status",
            "corrections",
            "error_codes",
            "error_details",
            "source_run_id",
            "source_file",
            "source_row_number",
            "engine_used",
        ],
        "properties": {
            "order_id": {
                "bsonType": "string",
                "minLength": 1,
            },
            "customer_id": {
                "bsonType": "string",
                "minLength": 1,
            },
            "quality_status": {
                "enum": [
                    "valid",
                    "corrected",
                ],
            },
            "corrections": {
                "bsonType": "array",
            },
            "error_codes": {
                "bsonType": "array",
            },
            "error_details": {
                "bsonType": "array",
            },
            "source_run_id": {
                "bsonType": "string",
            },
            "source_file": {
                "bsonType": "string",
            },
            "source_row_number": {
                "bsonType": "long",
            },
            "engine_used": {
                "bsonType": "string",
            },
        },
    }
}


def get_db(uri, database):
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=10000,
    )
    return client[database]


def setup_collections(db):

    # ---------------------------------------------------------
    # 1. orders_raw
    #
    # Raw data intentionally has:
    # - no schema validator
    # - no unique business-key constraint
    # ---------------------------------------------------------

    if "orders_raw" not in db.list_collection_names():
        db.create_collection("orders_raw")

    # ---------------------------------------------------------
    # 2. orders_validated
    #
    # Apply schema validation.
    # ---------------------------------------------------------

    if "orders_validated" not in db.list_collection_names():

        db.create_collection(
            "orders_validated",
            validator=VALIDATED_VALIDATOR,
            validationLevel="strict",
            validationAction="error",
        )

    else:

        db.command(
            "collMod",
            "orders_validated",
            validator=VALIDATED_VALIDATOR,
            validationLevel="strict",
            validationAction="error",
        )

    # Unique business key for idempotency.
    db.orders_validated.create_index(
        [("order_id", ASCENDING)],
        unique=True,
        name="uq_order_id",
    )

    # ---------------------------------------------------------
    # 3. orders_quarantine
    #
    # Quarantine records are uniquely identified by:
    # run + source row.
    # ---------------------------------------------------------

    if "orders_quarantine" not in db.list_collection_names():
        db.create_collection("orders_quarantine")

    db.orders_quarantine.create_index(
        [
            ("source_run_id", ASCENDING),
            ("source_row_number", ASCENDING),
        ],
        unique=True,
        name="uq_quarantine_source",
    )


def close_client(db):
    try:
        db.client.close()
    except Exception:
        pass