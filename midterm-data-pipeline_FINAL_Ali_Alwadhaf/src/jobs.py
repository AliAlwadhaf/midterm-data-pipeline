from datetime import datetime, timezone

from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

from src.materialized_views import refresh_materialized_views
from src.aggregations import AGGREGATIONS


# ============================================================
# MongoDB
# ============================================================

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "midterm_orders"
JOBS_COLLECTION = "job_runs"


# ============================================================
# Database
# ============================================================

def get_db():
    client = MongoClient(MONGO_URI)
    return client, client[DB_NAME]


# ============================================================
# Job Logging
# ============================================================

def log_job_start(db, job_name):
    started_at = datetime.now(timezone.utc)

    result = db[JOBS_COLLECTION].insert_one({
        "job_name": job_name,
        "status": "running",
        "started_at": started_at,
        "finished_at": None,
        "duration_seconds": None,
        "result": None,
        "error": None
    })

    return result.inserted_id, started_at


def log_job_end(
    db,
    job_id,
    started_at,
    status,
    result=None,
    error=None
):
    finished_at = datetime.now(timezone.utc)

    duration = (
        finished_at - started_at
    ).total_seconds()

    db[JOBS_COLLECTION].update_one(
        {"_id": job_id},
        {
            "$set": {
                "status": status,
                "finished_at": finished_at,
                "duration_seconds": duration,
                "result": result,
                "error": error
            }
        }
    )


# ============================================================
# Job 1
# Refresh Materialized Views
# ============================================================

def run_refresh_materialized_views():

    client, db = get_db()

    job_id, started_at = log_job_start(
        db,
        "refresh_materialized_views"
    )

    try:

        result = refresh_materialized_views()

        log_job_end(
            db,
            job_id,
            started_at,
            "success",
            result=result
        )

        return result

    except Exception as e:

        log_job_end(
            db,
            job_id,
            started_at,
            "failed",
            error=str(e)
        )

        raise

    finally:
        client.close()


# ============================================================
# Job 2
# Run Aggregation Report
# ============================================================

def run_aggregation_report(
    report_name="sales_by_payment_status"
):

    client, db = get_db()

    job_id, started_at = log_job_start(
        db,
        f"aggregation:{report_name}"
    )

    try:

        if report_name not in AGGREGATIONS:
            raise ValueError(
                f"Unknown aggregation: {report_name}"
            )

        function = AGGREGATIONS[report_name]

        if report_name in [
            "sales_by_city",
            "sales_by_customer",
            "daily_sales"
        ]:
            result = function(5)

        else:
            result = function()

        summary = {
            "report_name": report_name,
            "rows_returned": len(result),
            "sample": result[:5]
        }

        log_job_end(
            db,
            job_id,
            started_at,
            "success",
            result=summary
        )

        return summary

    except Exception as e:

        log_job_end(
            db,
            job_id,
            started_at,
            "failed",
            error=str(e)
        )

        raise

    finally:
        client.close()


# ============================================================
# Available Jobs
# ============================================================

JOBS = {
    "refresh_materialized_views":
        run_refresh_materialized_views,

    "run_aggregation_report":
        run_aggregation_report
}


# ============================================================
# Job Schedules
# ============================================================

JOB_SCHEDULES = {
    "refresh_materialized_views":
        "Every 1 hour",

    "run_aggregation_report":
        "Every 6 hours"
}


# ============================================================
# Manual Job Trigger
# ============================================================

def run_job(name):

    if name not in JOBS:
        raise ValueError(
            f"Unknown job: {name}"
        )

    return JOBS[name]()


# ============================================================
# List Jobs
# ============================================================

def list_jobs():

    return [
        {
            "name": name,
            "schedule": schedule
        }
        for name, schedule in JOB_SCHEDULES.items()
    ]


# ============================================================
# Real Scheduler
# ============================================================

def create_scheduler():

    scheduler = BackgroundScheduler(
        timezone="UTC"
    )

    # Every 1 hour
    scheduler.add_job(
        run_refresh_materialized_views,
        trigger="interval",
        hours=1,
        id="refresh_materialized_views",
        replace_existing=True,
        max_instances=1,
        coalesce=True
    )

    # Every 6 hours
    scheduler.add_job(
        run_aggregation_report,
        trigger="interval",
        hours=6,
        id="run_aggregation_report",
        replace_existing=True,
        max_instances=1,
        coalesce=True
    )

    return scheduler


# ============================================================
# Standalone Test
# ============================================================

if __name__ == "__main__":

    print("Available scheduled jobs:")

    for job in list_jobs():
        print(
            f"- {job['name']} "
            f"({job['schedule']})"
        )

    print("\nTesting manual trigger:")

    result = run_job(
        "run_aggregation_report"
    )

    print("\nJob result:")
    print(result)