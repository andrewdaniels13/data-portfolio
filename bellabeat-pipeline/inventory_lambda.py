"""Inventory Lambda for the Bellabeat pipeline.

First state in the Step Functions machine. Confirms every expected
raw CSV exists under the raw prefix and is non-empty before any EMR
cluster is created.

Event (from state input):
  {"bucket": "...",
   "raw_prefix": "bellabeat-raw/",
   "expected_files": ["dailyActivity_merged.csv", ...]}

Raises MissingInputFiles so the state machine can Catch it by name.
"""
import json


class MissingInputFiles(Exception):
    pass


def check_inventory(objects, raw_prefix, expected_files):
    """Pure check. objects: list of {"Key": str, "Size": int}."""
    found = {}
    for obj in objects:
        key = obj["Key"]
        if not key.startswith(raw_prefix):
            continue
        name = key[len(raw_prefix):]
        if "/" in name or name == "":
            continue
        found[name] = obj["Size"]

    missing = [f for f in expected_files if f not in found]
    empty = [f for f in expected_files
             if f in found and found[f] == 0]
    present = {f: found[f] for f in expected_files if f in found}
    return missing, empty, present


def list_objects(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def lambda_handler(event, context, s3=None):
    if s3 is None:
        import boto3
        s3 = boto3.client("s3")

    bucket = event["bucket"]
    prefix = event["raw_prefix"]
    expected = event["expected_files"]

    objects = list_objects(s3, bucket, prefix)
    missing, empty, present = check_inventory(
        objects, prefix, expected)

    result = {"bucket": bucket, "raw_prefix": prefix,
              "files": present, "missing": missing,
              "empty": empty}
    print("INVENTORY " + json.dumps(result))

    if missing or empty:
        raise MissingInputFiles(
            f"missing={missing} empty={empty}")

    return {"files_found": len(present), "files": present}
