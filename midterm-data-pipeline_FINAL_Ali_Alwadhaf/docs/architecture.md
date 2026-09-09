# Architecture

## Entry Point and Routing

The single entry point is `src/main.py`.

It measures the input file size and selects:

- `python_batch` for files up to and including 200 MB.
- `pyspark` for files larger than 200 MB.

The final large-file execution used PySpark for the 13 GB input.

## ELT Flow

The pipeline follows this order:

```text
Input CSV
   |
   v
File Router
   |
   +--------------------+
   |                    |
   v                    v
Python Batch          PySpark
   |                    |
   +---------+----------+
             |
             v
       orders_raw
             |
             v
      Quality Rules
             |
       +-----+------+
       |            |
       v            v
orders_validated  orders_quarantine
```

Every input record is written to the raw layer before quality processing.

## MongoDB

### `orders_raw`

Append-oriented raw collection. It has no strict validator or unique business-key index because raw ingestion must preserve the incoming records and allow repeated runs with different `run_id` values.

### `orders_validated`

Contains records classified as `valid` or `corrected`.

`order_id` is the stable business key and has a unique index. The pipeline uses replacement upsert semantics to support idempotent reruns and updates.

### `orders_quarantine`

Contains records that cannot be safely accepted. It retains the original/raw information together with error codes and details.

## Python Batch Path

The Python path reads the CSV as a stream and processes configurable batches. It does not materialize the complete input file in memory.

Batch progress reports include batch number, row count, elapsed time, and throughput.

## PySpark Path

The large-file path uses a fixed all-string schema for the input CSV and Spark partitions. The raw ingestion stage uses the MongoDB Spark Connector for parallel writes.

The quality processing runs per Spark partition so the full 13 GB input is not collected to the driver.

A duplicate-order rule requires partition-level processing by `order_id`; this is the justified shuffle used by the quality stage.

## Final Large-File Execution

The successful 13 GB execution processed:

- 30,000,000 input rows
- 99 input partitions
- 123,767 valid records
- 26,356,400 corrected records
- 3,519,833 quarantined records
- approximately 2,321.84 rows/second

The three quality outcomes sum to the full input count.
