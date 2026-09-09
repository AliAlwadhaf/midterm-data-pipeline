import csv
import time

from bson import Int64

from .quality_rules import clean_record


def _same_business_doc(old, new):
    """
    Compare business content while ignoring run-specific metadata.
    """

    old = dict(old)
    new = dict(new)

    old.pop("_id", None)
    new.pop("_id", None)

    for doc in (old, new):
        doc.pop("source_run_id", None)
        doc.pop("source_row_number", None)
        doc.pop("source_file", None)
        doc.pop("engine_used", None)
        doc.pop("ingested_at", None)

    return old == new


def _upsert_valid(doc, valid):
    """
    Atomic business-key upsert.

    Uses find_one_and_replace(..., upsert=True) instead of the
    non-atomic find_one -> insert_one pattern. The returned previous
    document is used only to classify the result.
    """
    from pymongo import ReturnDocument

    previous = valid.find_one_and_replace(
        {"order_id": doc["order_id"]},
        doc,
        upsert=True,
        return_document=ReturnDocument.BEFORE,
    )

    if previous is None:
        return "inserted"

    if _same_business_doc(previous, doc):
        return "unchanged"

    return "updated"

def _process(
    batch,
    run_id,
    source_file,
    valid,
    quarantine,
    counts,
    seen_order_ids,
):

    for row_number, raw in batch:

        doc = clean_record(raw)

        doc.update(
            {
                "source_run_id": run_id,
                "source_file": source_file,
                "source_row_number": Int64(row_number),
                "engine_used": "python_batch",
            }
        )

        # ---------------------------------------------------------
        # Quarantine from cleaning rules
        # ---------------------------------------------------------

        if doc["quality_status"] == "quarantined":

            doc["raw_record"] = dict(raw)

            key = {
                "source_run_id": run_id,
                "source_row_number": row_number,
            }

            quarantine.replace_one(
                key,
                doc,
                upsert=True,
            )

            counts["count_quarantine"] += 1

            for error_code in doc["error_codes"]:

                counts["counts_case_error"][error_code] = (
                    counts["counts_case_error"].get(error_code, 0)
                    + 1
                )

            continue

        # ---------------------------------------------------------
        # Duplicate order_id within the same run
        # ---------------------------------------------------------

        order_id = doc.get("order_id")

        already_in_run = (
            bool(order_id)
            and order_id in seen_order_ids
        )

        if order_id:
            seen_order_ids.add(order_id)

        if already_in_run:

            dup_doc = dict(doc)

            dup_doc["quality_status"] = "quarantined"

            dup_doc["error_codes"] = list(
                dict.fromkeys(
                    (dup_doc.get("error_codes") or [])
                    + ["ID_ORDER_DUPLICATE"]
                )
            )

            dup_doc["error_details"] = [
                "Duplicate order_id within the same run"
            ]

            dup_doc["raw_record"] = dict(raw)

            key = {
                "source_run_id": run_id,
                "source_row_number": row_number,
            }

            quarantine.replace_one(
                key,
                dup_doc,
                upsert=True,
            )

            counts["count_quarantine"] += 1

            for error_code in dup_doc["error_codes"]:

                counts["counts_case_error"][error_code] = (
                    counts["counts_case_error"].get(error_code, 0)
                    + 1
                )

            continue

        # ---------------------------------------------------------
        # Valid / corrected business record
        # ---------------------------------------------------------

        status = _upsert_valid(
            doc,
            valid,
        )

        if doc["quality_status"] == "corrected":
            counts["count_corrected"] += 1
        else:
            counts["count_valid"] += 1

        counts[f"count_{status}"] += 1


def load_batch(
    path,
    db,
    run_id,
    batch_size,
    source_file,
):

    raw_col = db.orders_raw
    valid = db.orders_validated
    quarantine = db.orders_quarantine

    counts = {
        "read_rows": 0,
        "loaded_raw": 0,
        "count_valid": 0,
        "count_corrected": 0,
        "count_quarantine": 0,
        "count_inserted": 0,
        "count_updated": 0,
        "count_unchanged": 0,
        "batches": 0,
        "counts_case_error": {},
    }

    started = time.perf_counter()

    # Tracks accepted order_ids during this run.
    seen_order_ids = set()

    with open(
        path,
        "r",
        encoding="utf-8-sig",
        newline="",
        errors="replace",
    ) as f:

        reader = csv.DictReader(f)

        batch = []

        for row_number, row in enumerate(
            reader,
            start=2,
        ):

            counts["read_rows"] += 1

            batch.append(
                (row_number, row)
            )

            if len(batch) >= batch_size:

                # -------------------------------------------------
                # RAW FIRST
                # -------------------------------------------------

                ingested_at = time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ",
                    time.gmtime(),
                )

                raw_docs = [
                    {
                        "run_id": run_id,
                        "source_file": source_file,
                        "source_row_number": row_number,
                        "ingested_at": ingested_at,
                        "engine_used": "python_batch",
                        "raw_record": dict(raw),
                    }
                    for row_number, raw in batch
                ]

                raw_col.insert_many(
                    raw_docs,
                    ordered=False,
                )

                counts["loaded_raw"] += len(
                    raw_docs
                )

                # -------------------------------------------------
                # CLEAN AFTER RAW
                # -------------------------------------------------

                _process(
                    batch,
                    run_id,
                    source_file,
                    valid,
                    quarantine,
                    counts,
                    seen_order_ids,
                )

                counts["batches"] += 1

                elapsed = (
                    time.perf_counter()
                    - started
                )

                throughput = (
                    counts["read_rows"]
                    / max(elapsed, 1e-9)
                )

                print(
                    f"Python batch "
                    f"{counts['batches']}: "
                    f"{len(batch)} rows | "
                    f"{elapsed:.2f}s | "
                    f"{throughput:.1f} rows/s"
                )

                batch = []

        # ---------------------------------------------------------
        # Final partial batch
        # ---------------------------------------------------------

        if batch:

            ingested_at = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime(),
            )

            raw_docs = [
                {
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_number,
                    "ingested_at": ingested_at,
                    "engine_used": "python_batch",
                    "raw_record": dict(raw),
                }
                for row_number, raw in batch
            ]

            raw_col.insert_many(
                raw_docs,
                ordered=False,
            )

            counts["loaded_raw"] += len(
                raw_docs
            )

            _process(
                batch,
                run_id,
                source_file,
                valid,
                quarantine,
                counts,
                seen_order_ids,
            )

            counts["batches"] += 1

    counts["seconds_elapsed"] = (
        time.perf_counter()
        - started
    )

    counts["throughput"] = (
        counts["read_rows"]
        / max(
            counts["seconds_elapsed"],
            1e-9,
        )
    )

    return counts