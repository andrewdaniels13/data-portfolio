"""Bellabeat Fitbit ETL (PySpark 3.5).

Joins four Fitbit tables (Apr 12 - May 12, 2016 export) into two
analytics-ready Parquet datasets:

  <output>/daily/   one row per user-day: activity + sleep + weight
  <output>/hourly/  one row per user-hour: steps, on the same valid days

Rules (measured in profile_inputs.py before this job was written):
  - sleepDay has 3 exact duplicate rows -> dropped before the join
  - a day with TotalSteps = 0 AND SedentaryMinutes = 1440 has no
    recorded signal -> removed from daily and from hourly
  - (Id, date) must be unique in every daily-grain input; the job
    fails loudly if it is not
  - session time zone is UTC, so timestamps read the same locally
    and on EMR

Usage:
  spark-submit fitbit_etl.py --input <dir> --output <dir>
  (local paths or s3:// URIs)
"""
import argparse
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType,
)

TS = "M/d/yyyy h:mm:ss a"


def schema(cols):
    return StructType([StructField(c, t) for c, t in cols])


ACT = schema([
    ("Id", LongType()),
    ("ActivityDate", StringType()),
    ("TotalSteps", IntegerType()),
    ("TotalDistance", DoubleType()),
    ("TrackerDistance", DoubleType()),
    ("LoggedActivitiesDistance", DoubleType()),
    ("VeryActiveDistance", DoubleType()),
    ("ModeratelyActiveDistance", DoubleType()),
    ("LightActiveDistance", DoubleType()),
    ("SedentaryActiveDistance", DoubleType()),
    ("VeryActiveMinutes", IntegerType()),
    ("FairlyActiveMinutes", IntegerType()),
    ("LightlyActiveMinutes", IntegerType()),
    ("SedentaryMinutes", IntegerType()),
    ("Calories", IntegerType()),
])

SLP = schema([
    ("Id", LongType()),
    ("SleepDay", StringType()),
    ("TotalSleepRecords", IntegerType()),
    ("TotalMinutesAsleep", IntegerType()),
    ("TotalTimeInBed", IntegerType()),
])

HR = schema([
    ("Id", LongType()),
    ("ActivityHour", StringType()),
    ("StepTotal", IntegerType()),
])

WT = schema([
    ("Id", LongType()),
    ("Date", StringType()),
    ("WeightKg", DoubleType()),
    ("WeightPounds", DoubleType()),
    ("Fat", DoubleType()),
    ("BMI", DoubleType()),
    ("IsManualReport", BooleanType()),
    ("LogId", LongType()),
])


def read(spark, base, name, sch):
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(sch)
        .csv(f"{base}/{name}_merged.csv")
    )


def assert_unique(df, name, keys):
    dups = df.groupBy(*keys).count().filter("count > 1").count()
    if dups:
        raise ValueError(f"{name}: {dups} duplicate {keys} groups")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("bellabeat-fitbit-etl")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    m = {}

    # --- activity (daily spine) ---
    act = read(spark, args.input, "dailyActivity", ACT)
    m["activity_rows_in"] = act.count()
    act = act.select(
        F.col("Id").alias("id"),
        F.to_date("ActivityDate", "M/d/yyyy").alias("activity_date"),
        F.col("TotalSteps").alias("total_steps"),
        F.col("TotalDistance").alias("total_distance"),
        F.col("VeryActiveMinutes").alias("very_active_minutes"),
        F.col("FairlyActiveMinutes").alias("fairly_active_minutes"),
        F.col("LightlyActiveMinutes").alias("lightly_active_minutes"),
        F.col("SedentaryMinutes").alias("sedentary_minutes"),
        F.col("Calories").alias("calories"),
    )
    assert_unique(act, "dailyActivity", ["id", "activity_date"])

    no_signal = (
        (F.col("total_steps") == 0)
        & (F.col("sedentary_minutes") == 1440)
    )
    act = act.filter(~no_signal)
    m["activity_rows_kept"] = act.count()
    m["zero_activity_removed"] = (
        m["activity_rows_in"] - m["activity_rows_kept"]
    )

    # --- sleep ---
    slp = read(spark, args.input, "sleepDay", SLP)
    m["sleep_rows_in"] = slp.count()
    slp = slp.dropDuplicates()
    m["sleep_exact_dups_removed"] = m["sleep_rows_in"] - slp.count()
    slp = slp.select(
        F.col("Id").alias("id"),
        F.to_date(F.to_timestamp("SleepDay", TS)).alias("activity_date"),
        F.col("TotalSleepRecords").alias("total_sleep_records"),
        F.col("TotalMinutesAsleep").alias("total_minutes_asleep"),
        F.col("TotalTimeInBed").alias("total_time_in_bed"),
    )
    assert_unique(slp, "sleepDay", ["id", "activity_date"])
    keys = ["id", "activity_date"]
    m["sleep_rows_unmatched"] = slp.join(act, keys, "left_anti").count()

    # --- weight ---
    wt = read(spark, args.input, "weightLogInfo", WT)
    m["weight_rows_in"] = wt.count()
    wt = wt.select(
        F.col("Id").alias("id"),
        F.to_date(F.to_timestamp("Date", TS)).alias("activity_date"),
        F.col("WeightKg").alias("weight_kg"),
        F.col("BMI").alias("bmi"),
        F.col("Fat").alias("fat_pct"),
        F.col("IsManualReport").alias("weight_is_manual"),
    )
    assert_unique(wt, "weightLogInfo", keys)
    m["weight_rows_unmatched"] = wt.join(act, keys, "left_anti").count()

    # --- daily join ---
    per_user = Window.partitionBy("id")
    daily = (
        act.join(slp, keys, "left")
        .join(wt, keys, "left")
        .withColumn("day_of_week", F.date_format("activity_date", "EEEE"))
        .withColumn(
            "logs_sleep",
            F.col("total_minutes_asleep").isNotNull().cast("int"))
        .withColumn(
            "logs_weight",
            F.col("weight_kg").isNotNull().cast("int"))
        .withColumn("days_logged", F.count("*").over(per_user))
        .withColumn("user_part", F.col("id"))
    )
    m["daily_rows_out"] = daily.count()
    m["daily_users"] = daily.select("id").distinct().count()
    m["users_with_sleep"] = (
        daily.filter("logs_sleep = 1").select("id").distinct().count())
    m["users_with_weight"] = (
        daily.filter("logs_weight = 1").select("id").distinct().count())
    if m["daily_rows_out"] != m["activity_rows_kept"]:
        raise ValueError("join changed the daily row count")

    # --- hourly, restricted to the valid days ---
    hr = read(spark, args.input, "hourlySteps", HR)
    m["hourly_rows_in"] = hr.count()
    ts = F.to_timestamp("ActivityHour", TS)
    hr = hr.select(
        F.col("Id").alias("id"),
        ts.alias("activity_hour"),
        F.to_date(ts).alias("activity_date"),
        F.hour(ts).alias("hour_of_day"),
        F.date_format(ts, "EEEE").alias("day_of_week"),
        F.col("StepTotal").alias("step_total"),
    )
    hr = (
        hr.join(act.select(*keys), keys, "left_semi")
        .withColumn("user_part", F.col("id"))
    )
    m["hourly_rows_out"] = hr.count()
    m["hourly_rows_removed"] = m["hourly_rows_in"] - m["hourly_rows_out"]

    # --- write: one file per user; id stays inside the files ---
    for name, df in (("daily", daily), ("hourly", hr)):
        (
            df.repartition("user_part").write.mode("overwrite")
            .partitionBy("user_part")
            .parquet(f"{args.output}/{name}/")
        )

    back = spark.read.parquet(f"{args.output}/daily/").count()
    if back != m["daily_rows_out"]:
        raise ValueError(f"daily read-back {back} != written")

    print("METRICS " + json.dumps(m))
    spark.stop()


if __name__ == "__main__":
    main()
