-- Cyclistic Analytics — schema as it actually ran
-- RDS PostgreSQL 18.3, db.t4g.small
-- Statements below are verbatim from the session that
-- built the table, in execution order.

-- 1. Table. 16 columns, matching the cleaned output of
-- the Lambda pipeline in ../cyclistic-pipeline/.
--
-- NOTE: has_station_names is created TEXT here, then cast
-- to BOOLEAN in step 3. That was a precaution, not a fix:
-- a distinct-value check later confirmed exactly two clean
-- values and no nulls, and Postgres accepts True/False on
-- COPY, so a BOOLEAN column would have loaded fine.
-- The detour is kept because it is what ran.

CREATE TABLE cyclistic_trips (
    ride_id            TEXT,
    rideable_type      TEXT,
    started_at         TIMESTAMP,
    ended_at           TIMESTAMP,
    start_station_name TEXT,
    start_station_id   TEXT,
    end_station_name   TEXT,
    end_station_id     TEXT,
    start_lat          DOUBLE PRECISION,
    start_lng          DOUBLE PRECISION,
    end_lat            DOUBLE PRECISION,
    end_lng            DOUBLE PRECISION,
    member_casual      TEXT,
    ride_length        DOUBLE PRECISION,
    day_of_week        TEXT,
    has_station_names  TEXT
);

-- 2. Load. Run from psql on the workstation, once per
-- monthly file. RDS PostgreSQL cannot COPY from s3://,
-- so this is client-side \copy, not server-side COPY.
-- Twelve files, 5,871,696 rows total.
--
-- \copy cyclistic_trips FROM '202508.csv' CSV HEADER

-- 3. Cast. 16.77 s full table rewrite.
ALTER TABLE cyclistic_trips
  ALTER COLUMN has_station_names TYPE BOOLEAN
  USING has_station_names::boolean;

-- 4. Maintenance. Not optional and not cosmetic.
-- The step-3 rewrite left the visibility map empty and
-- the stats stale. Running this BEFORE any baseline
-- measurement is what stops the index in step 5 being
-- credited with a 5.8x gain that was really this.
-- It is also what makes Heap Fetches: 0 possible.
VACUUM ANALYZE cyclistic_trips;

-- 5. Covering index. 16.35 s to build.
-- Columns are ordered to match the GROUP BY / ORDER BY
-- keys, with the aggregated column included, which gives
-- an Index Only Scan with no Sort node at all.
-- Measured effect on the day-of-week aggregation:
-- 14.53 s -> 3.21 s (4.5x).
-- This is query-shape specific, not a table-wide
-- speedup: the hour-of-day query filters on started_at,
-- which is not in the index, and still seq-scans.
CREATE INDEX idx_trips_member_dow_length
  ON cyclistic_trips (member_casual, day_of_week, ride_length);
