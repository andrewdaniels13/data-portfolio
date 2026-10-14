"""Profile the four Bellabeat input tables before writing the ETL job.

Reads each CSV with an explicit schema (FAILFAST, so a schema mismatch
errors instead of silently nulling), parses the date columns, and reports:
row counts, distinct users, unparsed dates, date range, duplicate keys,
and zero-activity day counts.

Usage:
    python profile_inputs.py "<path to Fitabase Data 4.12.16-5.12.16>"
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType,
)

DATA = sys.argv[1]
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

spark = (
    SparkSession.builder.master("local[*]")
    .appName("bellabeat-profile").getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def read(name, sch):
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(sch)
        .csv(f"{DATA}/{name}_merged.csv")
    )


activity = read("dailyActivity", ACT).withColumn(
    "d", F.to_date("ActivityDate", "M/d/yyyy"))
sleep = read("sleepDay", SLP).withColumn(
    "d", F.to_date(F.to_timestamp("SleepDay", TS)))
hourly = read("hourlySteps", HR).withColumn(
    "ts", F.to_timestamp("ActivityHour", TS)).withColumn(
    "d", F.to_date("ts"))
weight = read("weightLogInfo", WT).withColumn(
    "ts", F.to_timestamp("Date", TS)).withColumn(
    "d", F.to_date("ts"))


def profile(name, df, key):
    n = df.count()
    ids = df.select("Id").distinct().count()
    bad = df.filter(F.col("d").isNull()).count()
    lo, hi = df.agg(F.min("d"), F.max("d")).first()
    dups = df.groupBy("Id", *key).count().filter("count > 1")
    groups = dups.count()
    extra = dups.agg(F.sum(F.col("count") - 1)).first()[0] or 0
    exact = n - df.dropDuplicates().count()
    print(f"== {name}")
    print(f"  rows={n} users={ids} unparsed_dates={bad}")
    print(f"  date_range={lo} -> {hi}")
    print(f"  dup_key_groups={groups} dup_key_extra_rows={extra}")
    print(f"  exact_duplicate_rows={exact}")


profile("dailyActivity", activity, ["d"])
profile("sleepDay", sleep, ["d"])
profile("hourlySteps", hourly, ["ts"])
profile("weightLogInfo", weight, ["d"])

steps0 = F.col("TotalSteps") == 0
sed1440 = F.col("SedentaryMinutes") == 1440
print("== zero-activity candidates (dailyActivity)")
print(f"  TotalSteps=0: {activity.filter(steps0).count()} rows, "
      f"{activity.filter(steps0).select('Id').distinct().count()} users")
print(f"  SedentaryMinutes=1440: {activity.filter(sed1440).count()} rows")
print(f"  both: {activity.filter(steps0 & sed1440).count()} rows")

spark.stop()
