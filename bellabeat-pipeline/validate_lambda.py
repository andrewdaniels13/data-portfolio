"""Validation Lambda for the Bellabeat pipeline.

Runs after the PySpark step. For each output dataset it checks:
  - Spark wrote a _SUCCESS marker
  - at least one Parquet file exists
  - total rows (summed from Parquet footers) == expected rows
  - distinct user_part partitions == expected users
  - every file has an `id` column (Redshift COPY ignores
    Hive partition columns, so id must live inside the files)

Thresholds come from state input, not code:
  {"bucket": "...",
   "processed_prefix": "bellabeat-processed/",
   "datasets": {"daily":  {"rows": 868,   "users": 33},
                "hourly": {"rows": 20467, "users": 33}}}

All failures are collected, then raised together as
OutputValidationFailed so the state machine can Catch it by name.

Each Parquet object is fetched whole. Fine at this size
(a few MB total); a larger dataset would read footers only.
"""
import io
import json

import pyarrow.parquet as pq


class OutputValidationFailed(Exception):
    pass


PART_KEY = "user_part="


def summarize(keys, fetch, dataset_prefix):
    """keys: object keys under dataset_prefix.
    fetch: function key -> bytes."""
    has_success = (dataset_prefix + "_SUCCESS") in keys
    parquet = [k for k in keys if k.endswith(".parquet")]
    rows = 0
    users = set()
    missing_id = []
    for key in parquet:
        meta = pq.read_metadata(io.BytesIO(fetch(key)))
        rows += meta.num_rows
        if "id" not in meta.schema.names:
            missing_id.append(key)
        for part in key[len(dataset_prefix):].split("/"):
            if part.startswith(PART_KEY):
                users.add(part[len(PART_KEY):])
    return {"success_marker": has_success,
            "parquet_files": len(parquet),
            "rows": rows,
            "users": len(users),
            "files_missing_id": len(missing_id)}


def check(name, summary, expected):
    errors = []
    if not summary["success_marker"]:
        errors.append(f"{name}: no _SUCCESS marker")
    if summary["parquet_files"] == 0:
        errors.append(f"{name}: no parquet files")
    if summary["rows"] != expected["rows"]:
        errors.append(f"{name}: rows {summary['rows']}"
                      f" != expected {expected['rows']}")
    if summary["users"] != expected["users"]:
        errors.append(f"{name}: users {summary['users']}"
                      f" != expected {expected['users']}")
    if summary["files_missing_id"]:
        errors.append(f"{name}: {summary['files_missing_id']}"
                      " files missing id column")
    return errors


def list_keys(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(o["Key"] for o in page.get("Contents", []))
    return keys


def lambda_handler(event, context, s3=None):
    if s3 is None:
        import boto3
        s3 = boto3.client("s3")

    bucket = event["bucket"]
    base = event["processed_prefix"]

    def fetch(key):
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    report = {}
    errors = []
    for name, expected in event["datasets"].items():
        prefix = base + name + "/"
        keys = list_keys(s3, bucket, prefix)
        summary = summarize(keys, fetch, prefix)
        report[name] = summary
        errors.extend(check(name, summary, expected))

    print("VALIDATION " + json.dumps(
        {"report": report, "errors": errors}))

    if errors:
        raise OutputValidationFailed("; ".join(errors))

    return {"validated": True, "report": report}
