import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SMALL_FILE_THRESHOLD_MB = int(os.getenv("SMALL_FILE_THRESHOLD_MB", "200"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "midterm_orders")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
# Spark 4.x uses Scala 2.13; Connector 11.x supports Spark 4.x.
SPARK_CONNECTOR_PACKAGE = os.getenv("SPARK_CONNECTOR_PACKAGE", "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0")
REPORT_PATH = PROJECT_ROOT / "reports" / "results.json"
