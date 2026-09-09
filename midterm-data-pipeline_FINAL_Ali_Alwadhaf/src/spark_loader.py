"""
Distributed large-file path using Spark DataFrame API
and MongoDB Spark Connector.

Pipeline:
    CSV
      |
      +----> orders_raw
      |
      +----> quality processing
                |
                +----> orders_validated
                |
                +----> orders_quarantine
"""

import os
import sys
import time

from pymongo import MongoClient, ReturnDocument
from bson.int64 import Int64

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

from .quality_rules import clean_record


COLUMNS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json",
]

ERROR_CODES = [
    "ID_ORDER_MISSING",
    "ID_CUSTOMER_MISSING",
    "DATE_IMPOSSIBLE_INVALID",
    "JSON_ITEMS_CORRUPTED",
    "ITEMS_EMPTY",
    "PRICE_UNKNOWN",
    "VALUE_NEGATIVE_AMBIGUOUS",
    "ID_ORDER_DUPLICATE",
    "ERRORS_CONFLICTING_MULTIPLE",
    "INVALID_EMAIL",
    "INVALID_PHONE",
    "UNKNOWN_CURRENCY",
]


def _same_business_doc(old, new):
    """Compare business fields while ignoring ingestion metadata."""
    old = dict(old)
    new = dict(new)

    old.pop("_id", None)
    new.pop("_id", None)

    ignored_fields = [
        "source_run_id",
        "source_row_number",
        "source_file",
        "engine_used",
        "ingested_at",
    ]

    for doc in (old, new):
        for field in ignored_fields:
            doc.pop(field, None)

    return old == new


def _write_quality_partition(
    rows,
    uri,
    database,
    run_id,
    source_file,
    counters,
):
    """Clean records in one Spark partition."""

    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=10000,
    )

    db = client[database]

    local = {
        "count_valid": 0,
        "count_corrected": 0,
        "count_quarantine": 0,
        "count_inserted": 0,
        "count_updated": 0,
        "count_unchanged": 0,
    }

    local_error_counts = {}

    try:
        for spark_row in rows:

            row_dict = spark_row.asDict(
                recursive=True
            )

            source_row_number = row_dict.pop(
                "source_row_number",
                None,
            )

            is_duplicate = bool(
                row_dict.pop(
                    "_is_duplicate",
                    False,
                )
            )

            # IMPORTANT:
            # Do not modify items_json here.
            # Spark CSV parsing below handles the CSV quoting.
            raw = dict(row_dict)

            doc = clean_record(raw)

            # -------------------------------------------------
            # Duplicate handling
            # -------------------------------------------------

            if is_duplicate:

                if "ID_ORDER_DUPLICATE" not in doc["error_codes"]:
                    doc["error_codes"].append(
                        "ID_ORDER_DUPLICATE"
                    )

                doc["error_codes"] = sorted(
                    set(doc["error_codes"])
                )

                doc["error_details"].append(
                    "Duplicate order_id detected within the same input file."
                )

                doc["quality_status"] = "quarantined"

            # -------------------------------------------------
            # Metadata
            # -------------------------------------------------

            doc.update(
                {
                    "source_run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": Int64(source_row_number) if source_row_number is not None else None,
                    "engine_used": "pyspark",
                }
            )

            # -------------------------------------------------
            # Quarantine
            # -------------------------------------------------

            if doc["quality_status"] == "quarantined":

                doc["raw_record"] = raw

                db.orders_quarantine.replace_one(
                    {
                        "source_run_id": run_id,
                        "source_row_number": source_row_number,
                    },
                    doc,
                    upsert=True,
                )

                local["count_quarantine"] += 1

                for error_code in doc.get(
                    "error_codes",
                    [],
                ):
                    local_error_counts[error_code] = (
                        local_error_counts.get(
                            error_code,
                            0,
                        )
                        + 1
                    )

                continue

            # -------------------------------------------------
            # Validated
            # -------------------------------------------------

            order_id = doc.get("order_id")

            if not order_id:

                doc["quality_status"] = "quarantined"

                if "ID_ORDER_MISSING" not in doc["error_codes"]:
                    doc["error_codes"].append(
                        "ID_ORDER_MISSING"
                    )

                doc["error_codes"] = sorted(
                    set(doc["error_codes"])
                )

                doc["error_details"].append(
                    "order_id is empty."
                )

                doc["raw_record"] = raw

                db.orders_quarantine.replace_one(
                    {
                        "source_run_id": run_id,
                        "source_row_number": source_row_number,
                    },
                    doc,
                    upsert=True,
                )

                local["count_quarantine"] += 1

                for error_code in doc["error_codes"]:
                    local_error_counts[error_code] = (
                        local_error_counts.get(
                            error_code,
                            0,
                        )
                        + 1
                    )

                continue

            # Direct atomic idempotent upsert.
            # find_one_and_replace with upsert=True avoids the
            # non-idempotent "find -> insert" pattern while still
            # returning the previous document so we can classify the
            # result as inserted, updated, or unchanged.
            old = db.orders_validated.find_one_and_replace(
                {
                    "order_id": order_id
                },
                doc,
                upsert=True,
                return_document=ReturnDocument.BEFORE,
            )

            if old is None:
                local["count_inserted"] += 1
            elif _same_business_doc(old, doc):
                local["count_unchanged"] += 1
            else:
                local["count_updated"] += 1

            if doc["quality_status"] == "corrected":
                local["count_corrected"] += 1
            else:
                local["count_valid"] += 1

    finally:
        client.close()

    # Accumulators were created on the Driver.
    for key, value in local.items():
        counters[key].add(value)

    for error_code, value in local_error_counts.items():

        if error_code in counters["error_counts"]:
            counters["error_counts"][error_code].add(value)


def run_spark(
    path,
    db_uri,
    db_name,
    run_id,
    master="local[*]",
    connector_package=None,
):
    """Run the large-file Spark pipeline."""

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = "python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python.exe"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    builder = (
        SparkSession.builder
        .appName("MidtermOrdersPipeline")
        .master(master)
        .config(
            "spark.pyspark.python",
            python_executable,
        )
        .config(
            "spark.pyspark.driver.python",
            python_executable,
        )
        .config(
            "spark.python.worker.faulthandler.enabled",
            "true",
        )
        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "false",
        )
        .config(
            "spark.sql.debug.maxToStringFields",
            "200",
        )
    )

    if connector_package:
        builder = builder.config(
            "spark.jars.packages",
            connector_package,
        )

    spark = builder.getOrCreate()

    started = time.perf_counter()

    try:

        # =====================================================
        # 1. Fixed schema
        # =====================================================

        schema = StructType(
            [
                StructField(
                    column,
                    StringType(),
                    True,
                )
                for column in COLUMNS
            ]
        )

        # =====================================================
        # 2. Read CSV
        # =====================================================
        #
        # IMPORTANT FIX:
        #
        # items_json contains commas and doubled quotation
        # marks inside a CSV quoted field.
        #
        # The source uses " as both the CSV quote and escape
        # character, so Spark must be explicitly configured
        # to use the same character for escape.
        #
        # Without this, Spark was reading only:
        #
        # '"[{""sku"":""SKU-1010""'
        #
        # instead of the complete JSON field.
        # =====================================================

        df = (
            spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .option("multiLine", False)
            .option("quote", '"')
            .option("escape", '"')
            .schema(schema)
            .csv(path)
        )

        # =====================================================
        # 3. Input partitions
        # =====================================================

        partitions = df.rdd.getNumPartitions()

        print(
            f"Spark input partitions: {partitions}"
        )

        # =====================================================
        # 4. Physical plan
        # =====================================================

        print("== Physical Plan ==")
        df.explain(mode="formatted")

        # =====================================================
        # 5. Source row identifier
        # =====================================================

        source_row_expr = (
            F.monotonically_increasing_id()
            + F.lit(2)
        )

        # =====================================================
        # 6. RAW DATA
        # =====================================================

        raw_df = (
            df
            .withColumn(
                "source_row_number",
                source_row_expr,
            )
            .withColumn(
                "run_id",
                F.lit(run_id),
            )
            .withColumn(
                "source_file",
                F.lit(str(path)),
            )
            .withColumn(
                "ingested_at",
                F.current_timestamp(),
            )
            .withColumn(
                "engine_used",
                F.lit("pyspark"),
            )
            .withColumn(
                "raw_record",
                F.struct(
                    *[
                        F.col(column).alias(column)
                        for column in COLUMNS
                    ]
                ),
            )
            .select(
                "run_id",
                "source_file",
                "source_row_number",
                "ingested_at",
                "engine_used",
                "raw_record",
            )
        )

        # =====================================================
        # 7. RAW FIRST
        # =====================================================

        print(
            "Stage 1/2: Loading raw records to MongoDB..."
        )

        (
            raw_df.write
            .format("mongodb")
            .mode("append")
            .option(
                "database",
                db_name,
            )
            .option(
                "collection",
                "orders_raw",
            )
            .save()
        )

        print("Raw load completed.")

        # =====================================================
        # 8. Driver accumulators
        # =====================================================

        counter_names = [
            "count_valid",
            "count_corrected",
            "count_quarantine",
            "count_inserted",
            "count_updated",
            "count_unchanged",
        ]

        counters = {
            name: spark.sparkContext.accumulator(0)
            for name in counter_names
        }

        counters["error_counts"] = {}

        for code in ERROR_CODES:
            counters["error_counts"][code] = (
                spark.sparkContext.accumulator(0)
            )

        # =====================================================
        # 9. QUALITY DATAFRAME
        # =====================================================

        print(
            "Stage 2/2: Cleaning and validating records..."
        )

        quality_df = (
            df
            .withColumn(
                "source_row_number",
                source_row_expr,
            )
        )

        # =====================================================
        # 10. Duplicate detection
        # =====================================================

        duplicate_window = (
            Window
            .partitionBy("order_id")
            .orderBy(
                F.col("source_row_number")
            )
        )

        quality_df = (
            quality_df
            .withColumn(
                "_order_rank",
                F.row_number().over(
                    duplicate_window
                ),
            )
            .withColumn(
                "_is_duplicate",
                F.when(
                    (
                        F.col("order_id").isNotNull()
                        &
                        (
                            F.trim(
                                F.col("order_id")
                            )
                            != ""
                        )
                        &
                        (
                            F.col("_order_rank") > 1
                        )
                    ),
                    F.lit(True),
                ).otherwise(
                    F.lit(False)
                ),
            )
            .drop("_order_rank")
        )

        # =====================================================
        # 11. Distributed quality processing
        # =====================================================

        quality_df.foreachPartition(
            lambda partition: _write_quality_partition(
                partition,
                db_uri,
                db_name,
                run_id,
                str(path),
                counters,
            )
        )

        # =====================================================
        # 12. Metrics
        # =====================================================

        read_rows = (
            counters["count_valid"].value
            + counters["count_corrected"].value
            + counters["count_quarantine"].value
        )

        elapsed = time.perf_counter() - started

        result = {
            "read_rows": read_rows,
            "loaded_raw": read_rows,
            "input_partitions": partitions,
            "seconds_elapsed": elapsed,
            "throughput": (
                read_rows / max(elapsed, 1e-9)
            ),
        }

        for key in counter_names:
            result[key] = counters[key].value

        result["counts_case_error"] = {
            key: accumulator.value
            for key, accumulator
            in counters["error_counts"].items()
            if accumulator.value
        }

        return result

    finally:
        spark.stop()
