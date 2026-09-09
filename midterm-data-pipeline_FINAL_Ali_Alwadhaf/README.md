# Midterm Hybrid Orders Data Pipeline

## Project Scope

This project implements the required individual project path. It does not implement Advanced Path A or B.

## Implementation

`src/main.py` is the single entry point. It checks the input file size and routes:

- `<= 200 MB` → `python_batch`
- `> 200 MB` → `pyspark`

The router prints the file size, selected engine, and reason.

Both paths follow the required ELT order: every input record is first loaded into `orders_raw`, then quality rules are applied.

The final successful large-file run processed 30,000,000 records from the 13 GB source using PySpark with 99 input partitions.

## Project Structure

```text
midterm-data-pipeline/
├── config/
├── src/
├── tests/
├── reports/
├── docs/
├── data/
├── README.md
├── requirements.txt
└── .gitignore
```

Do not include the original 13 GB CSV in the final submission unless the instructor explicitly requires it. Include the 1,000-row sample.

## Requirements

- Python 3.10+
- Java compatible with the installed Spark version
- MongoDB
- Apache Spark / PySpark
- MongoDB Spark Connector

On Windows, activate the virtual environment with:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\Activate.ps1
```

Then:

```powershell
pip install -r requirements.txt
```

## MongoDB

Use the default local MongoDB URI:

```text
mongodb://localhost:27017
```

The database defaults to:

```text
midterm_orders
```

These can also be configured with environment variables:

```powershell
$env:MONGO_URI="mongodb://localhost:27017"
$env:MONGO_DATABASE="midterm_orders"
```

## Run the Small Sample

```powershell
python src\main.py --input data\orders_sample_1000.csv
```

Because the sample is below 200 MB, the router selects `python_batch`.

To change the batch size:

```powershell
python src\main.py --input data\orders_sample_1000.csv --batch-size 100
```

The final verification used a batch size of 500.

## Create a Sample from the Large File

Do not open the 13 GB CSV in Excel or load it into memory.

Use the streaming sample generator:

```powershell
python src\create_small_sample.py --input data\orders_huge_mixed_quality.csv --output data\orders_sample_1000.csv --rows 1000
```

## Run the 13 GB File

```powershell
python src\main.py --input data\orders_huge_mixed_quality.csv --master local[2]
```

The router automatically selects `pyspark` because the file is larger than 200 MB.

The successful final run recorded:

- Input rows: 30,000,000
- File size: 12,650.32 MB
- Engine: PySpark
- Input partitions: 99
- Valid: 123,767
- Corrected: 26,356,400
- Quarantined: 3,519,833
- Inserted: 26,479,173
- Updated: 0
- Unchanged: 994
- Throughput: approximately 2,321.84 rows/second

The three quality categories sum to exactly 30,000,000.

## ELT and MongoDB Collections

### `orders_raw`

Stores every input record before quality processing, together with ingestion metadata such as run ID, source file, source row information, and engine.

It intentionally has no business-key unique index or strict validator.

### `orders_validated`

Stores records classified as `valid` or `corrected`.

`order_id` is the stable business key and has a unique index. Writes use upsert semantics so rerunning the same input does not create duplicate business records.

### `orders_quarantine`

Stores records that cannot be safely accepted after the quality rules, together with error codes/details and the original raw record.

## Idempotency

Run the same sample twice.

The second final sample run produced:

- `count_inserted = 0`
- `count_updated = 0`
- `count_unchanged = 878`

This demonstrates that the business records were not duplicated on rerun.

The raw collection may grow between runs because each execution has a new `run_id`; this is intentional for the ELT raw layer.

## Quality Rules

The pipeline applies deterministic cleaning and validation rules including:

- trimming whitespace
- validating `order_id`
- validating `customer_id`
- normalizing numeric representations
- handling currency
- normalizing phone numbers
- validating email
- normalizing dates
- normalizing payment status
- parsing `items_json`
- recalculating totals from valid item data where appropriate
- detecting duplicate order IDs
- detecting conflicting multiple errors

The required error codes are recorded in the quarantine/error metadata.

Corrected records contain a `corrections` audit trail.

## Tests

Run:

```powershell
pytest -q
```

The final test run passed all 5 tests.

## Reports

`reports/results.json` stores the relevant execution metrics, including:

- `run_id`
- `read_rows`
- `loaded_raw`
- `count_valid`
- `count_corrected`
- `count_quarantine`
- elapsed time
- throughput
- engine
- batch size or Spark input partitions
- error counts
- inserted/updated/unchanged counts

See also:

- `reports/sample_analysis.md`
- `docs/architecture.md`

## Important Note

The 13 GB file should not be processed with Pandas or `list(reader)`. The large-file path uses Spark partitions, while the Python Batch path uses streaming batches.
