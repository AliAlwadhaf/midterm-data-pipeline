import os
import sys
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from bson import ObjectId
from datetime import datetime

from config.settings import MONGO_URI, MONGO_DATABASE
from src.mongo_setup import get_db
from src.aggregations import AGGREGATIONS
from src.materialized_views import refresh_materialized_views
from src.jobs import (
    list_jobs,
    run_job,
    create_scheduler
)


# ============================================================
# Project paths
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

API_UPLOAD_DIR = PROJECT_ROOT / "data" / "api_uploads"
API_UPLOAD_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# FastAPI Application
# ============================================================

app = FastAPI(
    title="Big Data Orders Analytics API",
    description=(
        "Unified FastAPI for the Big Data Orders project. "
        "Provides ingestion, indexes, queries, aggregations, "
        "materialized views, and scheduled jobs."
    ),
    version="1.0.0",
)


# ============================================================
# Background Scheduler
# ============================================================

scheduler = create_scheduler()


@app.on_event("startup")
def start_scheduler():
    """
    Start the real background scheduler when FastAPI starts.
    """

    if not scheduler.running:
        scheduler.start()

        print(
            "Background scheduler started."
        )

        print(
            "Scheduled jobs:"
        )

        print(
            "- refresh_materialized_views: Every 1 hour"
        )

        print(
            "- run_aggregation_report: Every 6 hours"
        )


@app.on_event("shutdown")
def stop_scheduler():
    """
    Stop the background scheduler when FastAPI shuts down.
    """

    if scheduler.running:
        scheduler.shutdown()

        print(
            "Background scheduler stopped."
        )


# ============================================================
# Helpers
# ============================================================

def get_database():
    """
    get_db() returns a MongoDB Database object directly.
    """

    return get_db(
        MONGO_URI,
        MONGO_DATABASE
    )


def make_json_safe(value):
    """
    Convert MongoDB/Python values to JSON-safe values.
    """

    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, dict):
        return {
            str(k): make_json_safe(v)
            for k, v in value.items()
        }

    if isinstance(value, list):
        return [
            make_json_safe(v)
            for v in value
        ]

    return value


def close_db(db):

    try:
        db.client.close()

    except Exception:
        pass


# ============================================================
# 1. HEALTH
# ============================================================

@app.get(
    "/health",
    tags=["System"]
)
def health():

    db = get_database()

    try:

        db.command("ping")

        return {
            "status": "healthy",
            "mongodb": "connected",
            "database": MONGO_DATABASE
        }

    except Exception as e:

        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "mongodb": "disconnected",
                "error": str(e)
            }
        )

    finally:

        close_db(db)


# ============================================================
# 2. ROOT
# ============================================================

@app.get(
    "/",
    tags=["System"]
)
def root():

    return {
        "project": "Big Data Orders Analytics",
        "api": "FastAPI",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


# ============================================================
# 3. INGEST
# ============================================================

@app.post(
    "/ingest",
    tags=["Ingestion"]
)
async def ingest(
    file: Optional[UploadFile] = File(
        default=None
    ),

    input_path: Optional[str] = Form(
        default=None
    ),

    force_engine: Optional[str] = Form(
        default=None
    )
):

    """
    Run the SAME pipeline used by src.main.

    Options:

    1. Upload a file through Swagger.

    2. Provide an existing local input_path.

    force_engine can be:
    - python_batch
    - pyspark
    """

    if file is None and not input_path:

        raise HTTPException(
            status_code=400,
            detail=(
                "Provide either "
                "'file' or 'input_path'."
            )
        )

    if force_engine not in (
        None,
        "python_batch",
        "pyspark"
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "force_engine must be "
                "python_batch or pyspark."
            )
        )

    # --------------------------------------------------------
    # Uploaded file
    # --------------------------------------------------------

    if file is not None:

        safe_name = Path(
            file.filename or "uploaded_file"
        ).name

        target = API_UPLOAD_DIR / safe_name

        if target.exists():

            import uuid

            target = (
                API_UPLOAD_DIR
                / f"{uuid.uuid4()}_{safe_name}"
            )

        try:

            with target.open("wb") as buffer:

                shutil.copyfileobj(
                    file.file,
                    buffer
                )

            input_path = str(target)

        except Exception as e:

            raise HTTPException(
                status_code=500,
                detail=(
                    f"Could not save "
                    f"uploaded file: {e}"
                )
            )

    # --------------------------------------------------------
    # Existing pipeline
    # --------------------------------------------------------

    command = [
        sys.executable,
        "-m",
        "src.main",
        "--input",
        input_path,
    ]

    if force_engine:

        command.extend(
            [
                "--force-engine",
                force_engine
            ]
        )

    try:

        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to start "
                f"ingestion pipeline: {e}"
            )
        )

    if result.returncode != 0:

        raise HTTPException(
            status_code=500,
            detail={
                "status": "failed",
                "return_code":
                    result.returncode,
                "stdout":
                    result.stdout[-5000:],
                "stderr":
                    result.stderr[-5000:],
            }
        )

    return {
        "status": "success",
        "message": (
            "Ingestion completed using "
            "the existing project pipeline."
        ),
        "input": input_path,
        "force_engine": force_engine,
        "output": result.stdout[-5000:],
    }


# ============================================================
# 4. INDEXES
# ============================================================

@app.post(
    "/indexes",
    tags=["Indexes"]
)
def create_indexes():

    db = get_database()

    try:

        collection = db[
            "orders_validated"
        ]

        created = {}

        created["customer_id"] = str(
            collection.create_index(
                [
                    ("customer_id", 1)
                ]
            )
        )

        created["city_order_date"] = str(
            collection.create_index(
                [
                    ("city", 1),
                    ("order_date", 1)
                ]
            )
        )

        created["payment_status"] = str(
            collection.create_index(
                [
                    ("payment_status", 1)
                ]
            )
        )

        created["source_row_number"] = str(
            collection.create_index(
                [
                    ("source_row_number", 1)
                ]
            )
        )

        return {
            "status": "success",
            "collection":
                "orders_validated",
            "indexes": created
        }

    finally:

        close_db(db)


# ============================================================
# 5. QUERIES
# ============================================================

QUERY_NAMES = [
    "order_by_id",
    "customer_orders",
    "city_date_orders",
    "payment_status_orders",
    "high_value_orders",
]


@app.get(
    "/queries",
    tags=["Queries"]
)
def get_queries():

    return {
        "count": len(QUERY_NAMES),
        "queries": QUERY_NAMES
    }


@app.get(
    "/queries/{name}",
    tags=["Queries"]
)
def execute_query(
    name: str,
    limit: int = 20,
    min_amount: float = 1_000_000,
):

    if name not in QUERY_NAMES:

        raise HTTPException(
            status_code=404,
            detail={
                "error": "Unknown query",
                "available_queries":
                    QUERY_NAMES
            }
        )

    if limit < 1 or limit > 1000:

        raise HTTPException(
            status_code=400,
            detail=(
                "limit must be between "
                "1 and 1000."
            )
        )

    db = get_database()

    collection = db[
        "orders_validated"
    ]

    try:

        # ----------------------------------------------------
        # Query 1
        # ----------------------------------------------------

        if name == "order_by_id":

            sample = collection.find_one(
                {},
                {
                    "_id": 0,
                    "order_id": 1
                }
            )

            if not sample:

                return {
                    "query": name,
                    "rows": []
                }

            order_id = sample[
                "order_id"
            ]

            rows = list(
                collection.find(
                    {
                        "order_id":
                            order_id
                    },
                    {
                        "_id": 0
                    }
                ).limit(limit)
            )

        # ----------------------------------------------------
        # Query 2
        # ----------------------------------------------------

        elif name == "customer_orders":

            sample = collection.find_one(
                {},
                {
                    "_id": 0,
                    "customer_id": 1
                }
            )

            if not sample:

                return {
                    "query": name,
                    "rows": []
                }

            customer_id = sample[
                "customer_id"
            ]

            rows = list(
                collection.find(
                    {
                        "customer_id":
                            customer_id
                    },
                    {
                        "_id": 0
                    }
                ).limit(limit)
            )

        # ----------------------------------------------------
        # Query 3
        # Compound index
        # ----------------------------------------------------

        elif name == "city_date_orders":

            sample = collection.find_one(
                {},
                {
                    "_id": 0,
                    "city": 1,
                    "order_date": 1
                }
            )

            if not sample:

                return {
                    "query": name,
                    "rows": []
                }

            city = sample.get(
                "city"
            )

            order_date = sample.get(
                "order_date",
                ""
            )

            year = str(
                order_date
            )[:4]

            start_date = (
                f"{year}-01-01"
            )

            end_date = (
                f"{int(year) + 1}-01-01"
            )

            rows = list(
                collection.find(
                    {
                        "city": city,
                        "order_date": {
                            "$gte":
                                start_date,
                            "$lt":
                                end_date
                        }
                    },
                    {
                        "_id": 0
                    }
                ).limit(limit)
            )

        # ----------------------------------------------------
        # Query 4
        # ----------------------------------------------------

        elif name == "payment_status_orders":

            sample = collection.find_one(
                {},
                {
                    "_id": 0,
                    "payment_status": 1
                }
            )

            if not sample:

                return {
                    "query": name,
                    "rows": []
                }

            payment_status = sample[
                "payment_status"
            ]

            rows = list(
                collection.find(
                    {
                        "payment_status":
                            payment_status
                    },
                    {
                        "_id": 0
                    }
                ).limit(limit)
            )

        # ----------------------------------------------------
        # Query 5
        # ----------------------------------------------------

        elif name == "high_value_orders":

            rows = list(
                collection.find(
                    {
                        "total_amount": {
                            "$gte":
                                min_amount
                        }
                    },
                    {
                        "_id": 0
                    }
                )
                .sort(
                    "total_amount",
                    -1
                )
                .limit(limit)
            )

        return {
            "query": name,
            "limit": limit,
            "rows_returned": len(rows),
            "rows":
                make_json_safe(rows)
        }

    finally:

        close_db(db)


# ============================================================
# 6. AGGREGATIONS
# ============================================================

@app.get(
    "/aggregations",
    tags=["Aggregations"]
)
def get_aggregations():

    names = list(
        AGGREGATIONS.keys()
    )

    return {
        "count": len(names),
        "aggregations": names
    }


@app.get(
    "/aggregations/{name}",
    tags=["Aggregations"]
)
def execute_aggregation(
    name: str,
    limit: int = 20,
):

    if name not in AGGREGATIONS:

        raise HTTPException(
            status_code=404,
            detail={
                "error":
                    "Unknown aggregation",
                "available_aggregations":
                    list(
                        AGGREGATIONS.keys()
                    )
            }
        )

    if limit < 1 or limit > 1000:

        raise HTTPException(
            status_code=400,
            detail=(
                "limit must be between "
                "1 and 1000."
            )
        )

    function = AGGREGATIONS[name]

    try:

        if name in [
            "sales_by_city",
            "sales_by_customer",
            "daily_sales"
        ]:

            result = function(
                limit
            )

        else:

            result = function()

        return {
            "aggregation": name,
            "rows_returned":
                len(result),
            "rows":
                make_json_safe(
                    result[:limit]
                )
        }

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail={
                "aggregation":
                    name,
                "error":
                    str(e)
            }
        )


# ============================================================
# 7. MATERIALIZED VIEWS
# ============================================================

@app.post(
    "/refresh-mv",
    tags=["Materialized Views"]
)
def refresh_mv():

    try:

        result = (
            refresh_materialized_views()
        )

        return {
            "status": "success",
            "result":
                make_json_safe(result)
        }

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail={
                "status": "failed",
                "error": str(e)
            }
        )


# ============================================================
# 8. JOBS
# ============================================================

@app.get(
    "/jobs",
    tags=["Jobs"]
)
def get_jobs():

    return {
        "jobs": list_jobs()
    }


@app.post(
    "/jobs/{name}/run",
    tags=["Jobs"]
)
def execute_job(name: str):

    try:

        result = run_job(
            name
        )

        return {
            "status": "success",
            "job": name,
            "result":
                make_json_safe(result)
        }

    except ValueError as e:

        raise HTTPException(
            status_code=404,
            detail=str(e)
        )

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail={
                "job": name,
                "status": "failed",
                "error": str(e)
            }
        )