# Big Data Final Project – Hybrid ELT Data Pipeline

## 1. Project Overview

This project is a hybrid ELT data pipeline for processing large-scale and mixed-quality order data.

The project was originally developed for the Big Data Midterm Project and was extended for the Final Project without rebuilding the original pipeline.

The system supports:

- Large-scale CSV ingestion.
- Data validation and correction.
- Data quality classification.
- MongoDB storage.
- Python batch processing for smaller files.
- Apache Spark processing for large files.
- MongoDB queries and indexes.
- Query execution analysis using `executionStats`.
- Aggregation reports.
- Incremental materialized views.
- Scheduled background jobs.
- A unified FastAPI service.
- Swagger/OpenAPI documentation.
- Automated testing.

The system is designed to work with different datasets and does not depend on hardcoded test values.

---

## 2. Project Structure

```text
midterm-data-pipeline/
|
+-- config/
|   +-- settings.py
|
+-- src/
|   +-- main.py
|   +-- file_router.py
|   +-- batch_loader.py
|   +-- spark_loader.py
|   +-- mongo_setup.py
|   +-- metrics.py
|   +-- aggregations.py
|   +-- materialized_views.py
|   +-- jobs.py
|   +-- api.py
|   +-- ...
|
+-- tests/
|
+-- reports/
|
+-- docs/
|
+-- data/
|   +-- orders_sample_1000.csv
|
+-- README.md
+-- requirements.txt
+-- example.env
+-- .gitignore
```

The large input dataset is intentionally excluded from GitHub because of its size.

---

## 3. Technologies

The project uses the following technologies:

- Python
- PySpark
- Apache Spark
- MongoDB
- PyMongo
- FastAPI
- Uvicorn
- APScheduler
- Pytest
- PowerShell on Windows

---

## 4. Requirements

### Software Requirements

Install the following:

- Python 3.10 or newer
- Java
- Apache Spark
- MongoDB
- Hadoop WinUtils for Spark on Windows

Python dependencies are listed in:

```text
requirements.txt
```

Install them using:

```powershell
pip install -r requirements.txt
```

---

## 5. Environment Configuration

Create a `.env` file if environment variables are required.

An example configuration is provided in:

```text
example.env
```

Example:

```env
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=midterm_orders
```

Do not commit real credentials or secrets to GitHub.

---

## 6. MongoDB Configuration

The default MongoDB connection is:

```text
mongodb://localhost:27017
```

The project database is:

```text
midterm_orders
```

Main collections include:

```text
orders_raw
orders_validated
orders_quarantine
job_runs
materialized_view_state
daily_sales_summary
top_products_summary
```

---

## 7. Large-Scale Data Pipeline

The original midterm pipeline supports hybrid processing.

The file router determines the appropriate processing engine based on the input file size.

### Python Batch Engine

Smaller files can be processed using the Python batch engine.

### PySpark Engine

Large files are processed using Apache Spark.

The project was tested with a large dataset containing approximately:

```text
30,000,000 rows
```

The large CSV file is not included in the GitHub repository because of its size.

---

## 8. Running the Original Pipeline

The main entry point is:

```text
src/main.py
```

Example:

```powershell
python -m src.main --input data/orders_sample_1000.csv
```

For a large input file:

```powershell
python -m src.main --input data/orders_huge_mixed_quality.csv
```

The pipeline automatically selects the appropriate engine according to the configured threshold.

The engine can also be forced.

Example:

```powershell
python -m src.main --input data/orders_sample_1000.csv --force-engine python_batch
```

For Spark:

```powershell
python -m src.main --input data/orders_huge_mixed_quality.csv --force-engine pyspark
```

---

## 9. Data Quality Processing

The pipeline separates records into different quality categories.

The main categories include:

- Valid records.
- Corrected records.
- Quarantined records.

The pipeline also records information such as:

- Quality status.
- Correction information.
- Error codes.
- Error details.
- Source run ID.
- Source file.
- Source row number.
- Processing engine.

This makes the ingestion process traceable and reproducible.

---

# Final Project Requirements

## 10. Queries and Indexes

The final project adds multiple MongoDB queries and indexes.

The project contains at least five query examples.

The available query names are:

```text
order_by_id
customer_orders
city_date_orders
payment_status_orders
high_value_orders
```

The project also creates multiple indexes.

Important indexes include:

```text
customer_id_1
city_1_order_date_1
payment_status_1
source_row_number_1
```

The compound index is:

```text
city_1_order_date_1
```

The project also maintains a unique index on:

```text
order_id
```

---

## 11. Query Execution Analysis

MongoDB `executionStats` is used to compare query execution before and after indexing.

At least three queries were analyzed.

The comparison considers:

- `nReturned`
- `totalKeysExamined`
- `totalDocsExamined`
- `executionTimeMillis`

The indexes reduce collection scanning when the query predicates are selective.

For example, the customer query changed from scanning the full collection to using the customer index and examining only the matching document.

For queries that return a very large percentage of the collection, MongoDB may still need to examine many documents even when an index is available. This is expected because the query is not highly selective.

---

## 12. Aggregation Reports

The project provides at least five independently runnable aggregation reports.

The available reports are:

```text
sales_by_city
sales_by_customer
sales_by_payment_status
orders_by_status
daily_sales
```

They are implemented in:

```text
src/aggregations.py
```

The reports can be executed independently through the aggregation mapping.

### Sales by City

Calculates sales information grouped by city.

### Sales by Customer

Calculates order and sales information grouped by customer.

### Sales by Payment Status

Calculates:

- Number of orders.
- Total sales.
- Average sales.

Grouped by payment status.

### Orders by Status

Counts orders grouped by order status.

### Daily Sales

Calculates daily order counts and total sales.

---

## 13. Materialized Views

The project implements two materialized views.

Required views:

```text
daily_sales_summary
top_products_summary
```

The implementation is located in:

```text
src/materialized_views.py
```

### daily_sales_summary

This view contains summarized sales information by date, including:

- Date.
- Order count.
- Total sales.

### top_products_summary

This view contains product-level summary information, including:

- SKU.
- Product name.
- Quantity.
- Total sales.

---

## 14. Incremental Materialized View Updates

The materialized views are updated incrementally instead of rebuilding the entire views every time.

The implementation maintains state in:

```text
materialized_view_state
```

The state tracks the last processed source row.

When the refresh operation runs again, only new records after the last processed position are processed.

The first refresh processed the existing dataset.

A subsequent refresh with no new input processed:

```text
0 records
```

This demonstrates the incremental update mechanism.

The refresh function is:

```text
refresh_materialized_views()
```

---

## 15. Scheduled Jobs

The project implements scheduled background jobs using APScheduler.

The implementation is located in:

```text
src/jobs.py
```

The available jobs are:

```text
refresh_materialized_views
run_aggregation_report
```

Schedules:

```text
refresh_materialized_views
    Every 1 hour

run_aggregation_report
    Every 6 hours
```

Each job records:

- Job name.
- Status.
- Start time.
- End time.
- Duration.
- Result.
- Error information.

The job logs are stored in:

```text
job_runs
```

---

## 16. Manual Job Trigger

Jobs can also be executed manually.

Run:

```powershell
python -m src.jobs
```

The command lists the available jobs and performs a manual test.

The aggregation job uses:

```text
sales_by_payment_status
```

as its default report.

---

## 17. FastAPI Unified Service

The project provides a unified FastAPI service.

The API implementation is located in:

```text
src/api.py
```

Start the API using:

```powershell
uvicorn src.api:app --reload
```

The API is available at:

```text
http://127.0.0.1:8000
```

Swagger documentation is available at:

```text
http://127.0.0.1:8000/docs
```

---

## 18. API Endpoints

### Health Check

```http
GET /health
```

Checks the API and MongoDB connection.

---

### Data Ingestion

```http
POST /ingest
```

Accepts a file upload or an existing input path.

The endpoint uses the same existing ingestion pipeline as the midterm project.

It does not implement a separate ingestion system.

---

### Create Indexes

```http
POST /indexes
```

Creates the required indexes.

---

### List Queries

```http
GET /queries
```

Returns the available query names.

---

### Run a Query

```http
GET /queries/{name}
```

Runs one of the registered queries.

Supported examples include:

```text
order_by_id
customer_orders
city_date_orders
payment_status_orders
high_value_orders
```

---

### List Aggregations

```http
GET /aggregations
```

Returns the available aggregation reports.

---

### Run an Aggregation

```http
GET /aggregations/{name}
```

Runs a selected aggregation report.

Examples:

```text
sales_by_city
sales_by_customer
sales_by_payment_status
orders_by_status
daily_sales
```

---

### Refresh Materialized Views

```http
POST /refresh-mv
```

Triggers the incremental materialized view refresh.

---

### List Jobs

```http
GET /jobs
```

Returns the available scheduled jobs and their schedules.

---

### Run a Job Manually

```http
POST /jobs/{name}/run
```

Triggers a selected job manually.

Example:

```text
POST /jobs/run_aggregation_report/run
```

---

## 19. Background Scheduler

The FastAPI application starts the APScheduler background scheduler during application startup.

When the API starts, the scheduler registers:

```text
refresh_materialized_views
    Every 1 hour

run_aggregation_report
    Every 6 hours
```

The scheduler is stopped when the FastAPI application shuts down.

This provides actual scheduled execution in addition to the manual job trigger.

---

## 20. API Example

Start the API:

```powershell
uvicorn src.api:app --reload
```

Open:

```text
http://127.0.0.1:8000/docs
```

The Swagger interface can be used to test all required endpoints.

---

## 21. Testing

The project contains automated tests in:

```text
tests/
```

Run the test suite using:

```powershell
pytest -q
```

The tests cover the main midterm pipeline functionality.

---

## 22. Example Workflow

A typical workflow is:

### Step 1 – Start MongoDB

Make sure MongoDB is running.

### Step 2 – Activate the virtual environment

```powershell
.venv\Scripts\Activate.ps1
```

### Step 3 – Configure Spark on Windows

Example:

```powershell
$env:HADOOP_HOME="C:\hadoop"
$env:Path += ";C:\hadoop\bin"
$env:SPARK_LOCAL_IP="127.0.0.1"
$env:PYSPARK_PYTHON="python.exe"
$env:PYSPARK_DRIVER_PYTHON="python.exe"
```

### Step 4 – Run the pipeline

```powershell
python -m src.main --input data/orders_sample_1000.csv
```

### Step 5 – Start FastAPI

```powershell
uvicorn src.api:app --reload
```

### Step 6 – Open Swagger

```text
http://127.0.0.1:8000/docs
```

### Step 7 – Create indexes

Use:

```http
POST /indexes
```

### Step 8 – Test queries

Use:

```http
GET /queries
```

and:

```http
GET /queries/{name}
```

### Step 9 – Test aggregations

Use:

```http
GET /aggregations
```

and:

```http
GET /aggregations/{name}
```

### Step 10 – Refresh materialized views

Use:

```http
POST /refresh-mv
```

### Step 11 – Test scheduled jobs manually

Use:

```http
GET /jobs
```

and:

```http
POST /jobs/{name}/run
```

---

## 23. Large Dataset Processing Results

The pipeline was tested with a large CSV dataset containing approximately:

```text
30,000,000 rows
```

The successful Spark processing produced approximately:

```text
30,000,000 raw rows
123,767 valid rows
26,356,400 corrected rows
3,519,833 quarantined rows
```

The validated collection contains approximately:

```text
26.48 million records
```

The pipeline also supports rerunning the same input without inserting duplicate records.

---

## 24. Incremental Processing Result

The ingestion pipeline supports rerunning the same input.

The first large run inserted approximately:

```text
26.48 million records
```

A subsequent run of the same input detected existing records and avoided reinserting them.

This demonstrates idempotent behavior for repeated ingestion.

---

## 25. GitHub Repository

The project is maintained in a GitHub repository.

The repository contains:

- Source code.
- Configuration files.
- Tests.
- Documentation.
- Requirements.
- Sample data.

Large datasets, virtual environments, temporary uploads, and local secrets are excluded using `.gitignore`.

---

## 26. Files Excluded from GitHub

The following types of files are intentionally excluded:

```text
.venv/
__pycache__/
.pytest_cache/
.env
data/orders_huge_mixed_quality.csv
data/api_uploads/
*.log
```

The large dataset is excluded because it is approximately 13 GB.

A small sample dataset can be included for testing:

```text
data/orders_sample_1000.csv
```

---

## 27. Security Notes

Do not store passwords, API keys, database credentials, or other secrets directly in the source code.

Use environment variables or a local `.env` file.

The `.env` file is excluded from GitHub using `.gitignore`.

The repository provides:

```text
example.env
```

as a configuration template without secrets.

---

## 28. Final Project Summary

The final project extends the original Big Data midterm pipeline with the required advanced functionality:

1. MongoDB queries and indexes.
2. Query execution analysis using `executionStats`.
3. Five independent aggregation reports.
4. Two incremental materialized views.
5. Two scheduled background jobs.
6. Job execution logging.
7. Manual job triggering.
8. A unified FastAPI service.
9. Swagger/OpenAPI documentation.
10. Automated tests.
11. README and reproducible setup instructions.

The implementation reuses the existing midterm ingestion pipeline rather than rebuilding it.

The final system therefore provides a complete Big Data processing workflow from ingestion and validation to querying, aggregation, materialized views, scheduled processing, and API access.
