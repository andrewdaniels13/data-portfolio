"""Cyclistic trip-data ETL: AWS Lambda handler + locally testable cleaning logic.

Deployed: triggered by S3 ObjectCreated events on the raw prefix of the
pipeline bucket. Reads the new monthly CSV, cleans it with pandas, writes
the cleaned CSV to the processed prefix.

Local test (no AWS credentials needed):

    python lambda_function.py <path-to-monthly-csv> [output-csv]

Prints per-step row counts as JSON so the cost of every filter is visible.
"""

import json
import logging
import os
import sys
import urllib.parse
from io import StringIO

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# All three are overridable via Lambda environment variables (set in the
# CloudFormation template); defaults match the draft's single-bucket layout.
RAW_PREFIX = os.environ.get("RAW_PREFIX", "cyclistic-raw/")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "cyclistic-processed/")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET")  # unset -> same bucket

# Older Divvy releases used a different schema. Map legacy column names to
# the current ones; DataFrame.rename ignores keys that aren't present.
LEGACY_COLUMNS = {
    "trip_id": "ride_id",
    "start_time": "started_at",
    "end_time": "ended_at",
    "from_station_name": "start_station_name",
    "to_station_name": "end_station_name",
    "usertype": "member_casual",
}

# Legacy member labels -> current ones (applied after lowercasing).
LEGACY_MEMBER_VALUES = {"subscriber": "member", "customer": "casual"}

REQUIRED_COLUMNS = {
    "ride_id",
    "started_at",
    "ended_at",
    "start_station_name",
    "end_station_name",
    "member_casual",
}

MIN_RIDE_MINUTES = 1        # below this = false start (also catches negatives)
MAX_RIDE_MINUTES = 24 * 60  # above this = data error


def clean_trips(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Clean one monthly trip file. Returns (cleaned_df, per-step row counts).

    Pure pandas, no AWS dependencies, so it runs identically in the local
    venv and inside Lambda.
    """
    counts = {"input_rows": len(df)}

    df = df.rename(columns=LEGACY_COLUMNS)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Normalize rider labels (no-op on current files; converts legacy
    # Subscriber/Customer values if an old-schema file ever lands).
    df["member_casual"] = (
        df["member_casual"].str.strip().str.lower().replace(LEGACY_MEMBER_VALUES)
    )

    # Timestamps arrive with and without millisecond precision, sometimes in
    # the same file. format="mixed" parses per element instead of assuming
    # the first row's format applies to all rows.
    for col in ("started_at", "ended_at"):
        df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")

    before = len(df)
    df = df.dropna(subset=["started_at", "ended_at"])
    counts["dropped_null_or_unparseable_timestamps"] = before - len(df)

    df["ride_length"] = (df["ended_at"] - df["started_at"]).dt.total_seconds() / 60
    df["day_of_week"] = df["started_at"].dt.day_name()

    before = len(df)
    df = df[df["ride_length"] >= MIN_RIDE_MINUTES]
    counts["dropped_under_1_min_incl_negative"] = before - len(df)

    before = len(df)
    df = df[df["ride_length"] <= MAX_RIDE_MINUTES]
    counts["dropped_over_24_hr"] = before - len(df)

    # Null station names are NOT dropped. Verified on 202508: 53.8% of
    # electric-bike rides have a blank station vs 0.3% of classic-bike rides
    # (dockless e-bikes locked to public racks rather than docked). Dropping
    # them would delete a third of the month and gut any bike-type analysis.
    # Instead, flag them and let station-level queries filter at query time.
    df["has_station_names"] = (
        df["start_station_name"].notna() & df["end_station_name"].notna()
    )
    counts["rows_missing_station_name"] = int((~df["has_station_names"]).sum())

    df["ride_length"] = df["ride_length"].round(2)
    counts["output_rows"] = len(df)
    return df, counts


def lambda_handler(event, context):
    logger.info("pandas %s", pd.__version__)
    record = event["Records"][0]
    src_bucket = record["s3"]["bucket"]["name"]
    # S3 event keys are URL-encoded (spaces arrive as '+').
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    if not key.startswith(RAW_PREFIX):
        # Guard against a misconfigured trigger (e.g. firing on the processed
        # prefix, which would invoke this function in an infinite loop).
        logger.warning("Ignoring object outside raw prefix: %s", key)
        return {"skipped": key}

    # Client created inside the handler so importing this module locally
    # never touches AWS config.
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=src_bucket, Key=key)
    df = pd.read_csv(obj["Body"])

    cleaned, counts = clean_trips(df)

    dest_bucket = PROCESSED_BUCKET or src_bucket
    dest_key = PROCESSED_PREFIX + key[len(RAW_PREFIX):]

    buffer = StringIO()
    cleaned.to_csv(buffer, index=False)
    s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=buffer.getvalue())

    logger.info(
        "Processed s3://%s/%s -> s3://%s/%s | %s",
        src_bucket, key, dest_bucket, dest_key, json.dumps(counts),
    )
    return counts


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python lambda_function.py <input-csv> [output-csv]")

    frame = pd.read_csv(sys.argv[1])
    cleaned_df, run_counts = clean_trips(frame)

    print(f"pandas {pd.__version__}")
    print(json.dumps(run_counts, indent=2))
    print("\nDerived columns preview:")
    print(cleaned_df[["started_at", "ended_at", "ride_length", "day_of_week",
                      "member_casual", "has_station_names"]].head().to_string())
    print("\nRides by type:")
    print(cleaned_df["rideable_type"].value_counts().to_string())

    if len(sys.argv) > 2:
        cleaned_df.to_csv(sys.argv[2], index=False)
        print(f"\nWrote {sys.argv[2]} ({len(cleaned_df)} rows)")
