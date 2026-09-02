-- Cyclistic Analytics — aggregation queries
-- RDS PostgreSQL 18.3. Run against cyclistic_trips
-- (5,871,696 rows). See schema.sql for DDL.
--
-- Results from these queries are recorded in the
-- README and drove all five Tableau views. The five
-- CSVs in data/ are the exported outputs.

-- 1. Duration and volume by user type and day of week.
-- Feeds Tableau view v1. 14 rows.
-- Mean, median and p90 together because the casual
-- distribution has a long right tail that pulls the
-- mean well above the median.
SELECT
  member_casual,
  day_of_week,
  COUNT(*) AS total_rides,
  ROUND(AVG(ride_length)::numeric, 2) AS avg_minutes,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
    (ORDER BY ride_length)::numeric, 2) AS median_minutes,
  ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP
    (ORDER BY ride_length)::numeric, 2) AS p90_minutes
FROM cyclistic_trips
GROUP BY member_casual, day_of_week
ORDER BY member_casual, day_of_week;

-- 2. Hour of day, as a percentage of each group's own
-- total. Feeds Tableau view v2. 48 rows (24 x 2).
-- Percentages are within-group, so the two series are
-- comparable despite members being 1.84x more numerous.
-- NOTE: started_at is not in the covering index, so
-- this query seq-scans and stays slow (~10 s). That is
-- the honest limit of a query-shape-specific index.
SELECT
  EXTRACT(HOUR FROM started_at)::int AS hour_of_day,
  COUNT(*) FILTER (WHERE member_casual = 'member') AS member_rides,
  COUNT(*) FILTER (WHERE member_casual = 'casual') AS casual_rides,
  ROUND(100.0 * COUNT(*) FILTER (WHERE member_casual = 'member')
    / SUM(COUNT(*) FILTER (WHERE member_casual = 'member'))
    OVER (), 2) AS member_pct,
  ROUND(100.0 * COUNT(*) FILTER (WHERE member_casual = 'casual')
    / SUM(COUNT(*) FILTER (WHERE member_casual = 'casual'))
    OVER (), 2) AS casual_pct
FROM cyclistic_trips
GROUP BY 1
ORDER BY 1;

-- 3. Morning window, split weekday vs weekend.
-- Confound check on query 2: is the morning gap real,
-- or just an artifact of casual riding being weekend-
-- weighted? Answer: real. Weekday 06-09 runs 4.13x
-- member-skewed against a 1.84x dataset baseline.
SELECT
  CASE WHEN day_of_week IN ('Saturday','Sunday')
    THEN 'weekend' ELSE 'weekday' END AS day_type,
  member_casual,
  EXTRACT(HOUR FROM started_at)::int AS hour_of_day,
  COUNT(*) AS rides,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY
    CASE WHEN day_of_week IN ('Saturday','Sunday')
      THEN 'weekend' ELSE 'weekday' END,
    member_casual), 2) AS pct_of_group
FROM cyclistic_trips
WHERE EXTRACT(HOUR FROM started_at) BETWEEN 6 AND 9
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- 4. Top 10 start stations per user type.
-- Feeds Tableau view v3. 20 rows.
-- WHERE has_station_names is a bare boolean predicate,
-- which only works because of the cast in schema.sql
-- step 3. This filter is mandatory: 32.4% of rows have
-- no station name (dockless e-bike rides).
WITH ranked AS (
  SELECT
    member_casual,
    start_station_name,
    COUNT(*) AS rides,
    ROW_NUMBER() OVER (PARTITION BY member_casual
      ORDER BY COUNT(*) DESC) AS rn
  FROM cyclistic_trips
  WHERE has_station_names
  GROUP BY member_casual, start_station_name
)
SELECT member_casual, rn, start_station_name, rides
FROM ranked
WHERE rn <= 10
ORDER BY member_casual, rn;

-- 5. Bike type by user type. Feeds Tableau view v5.
-- 4 rows: only classic_bike and electric_bike exist in
-- this data window. docked_bike does not appear.
-- NO station filter here, deliberately. Filtering on
-- has_station_names would systematically drop dockless
-- e-bike rides and corrupt the comparison.
-- The medians are the point: the casual/member gap is
-- 1.19x within electric but 1.73x within classic.
SELECT
  member_casual,
  rideable_type,
  COUNT(*) AS rides,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*))
    OVER (PARTITION BY member_casual), 2) AS pct_of_group,
  ROUND(AVG(ride_length)::numeric, 2) AS avg_minutes,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
    (ORDER BY ride_length)::numeric, 2) AS median_minutes
FROM cyclistic_trips
GROUP BY member_casual, rideable_type
ORDER BY member_casual, rideable_type;

-- 6. Monthly volume and casual share.
-- Feeds Tableau view v4.
-- Returns 13 months, not 12: Divvy names its files by
-- END month, so 99 rides that started before midnight
-- on 2025-07-31 sit inside the 202508 file. Those 99
-- are excluded from the Tableau chart (they render as
-- a false vertical launch from zero) but the exclusion
-- is footnoted wherever the chart appears.
SELECT
  TO_CHAR(started_at, 'YYYY-MM') AS month,
  COUNT(*) FILTER (WHERE member_casual = 'member') AS member_rides,
  COUNT(*) FILTER (WHERE member_casual = 'casual') AS casual_rides,
  ROUND(100.0 * COUNT(*) FILTER (WHERE member_casual = 'casual')
    / COUNT(*), 2) AS casual_share_pct
FROM cyclistic_trips
GROUP BY 1
ORDER BY 1;
