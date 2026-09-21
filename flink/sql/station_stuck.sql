-- AirPulse Job 4: stuck-sensor detection (event-time OVER window).
-- A separate sql-client session from station_aqi.sql - temporary views and
-- catalog objects don't survive across invocations - but reads the same raw
-- topic under its own consumer group, so the two scripts never interfere.
-- Submit (from Git Bash, prefix with MSYS_NO_PATHCONV=1):
--   docker exec airpulse-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/station_data_quality.sql

SET 'execution.checkpointing.interval' = '60 s';
SET 'table.local-time-zone' = 'UTC';
-- Kafka partitions with no traffic would otherwise hold the watermark back forever.
SET 'table.exec.source.idle-timeout' = '5 min';
-- LAG keeps state per (station, pollutant) key; expire keys nobody has touched in 2 days.
SET 'table.exec.state.ttl' = '48 h';

-- ---------------------------------------------------------------- SOURCE
CREATE TABLE readings_raw (
    schema_version INT,
    `source`       STRING,
    ingested_at    STRING,
    event_time     STRING,
    `raw` ROW<
        station STRING, pollutant_id STRING, city STRING, `state` STRING,
        latitude STRING, longitude STRING,
        min_value STRING, max_value STRING, avg_value STRING, last_update STRING
    >,
    ts AS TO_TIMESTAMP(SUBSTR(event_time, 1, 19), 'yyyy-MM-dd''T''HH:mm:ss'),
    WATERMARK FOR ts AS ts - INTERVAL '30' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'aqi.readings.raw',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'airpulse-flink-stuck',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'     -- smoke-test messages must not kill the job
);

-- A typed, cleaned view - same shape as station_aqi.sql's, minus the columns
-- this job doesn't need.
CREATE TEMPORARY VIEW readings AS
SELECT
    `raw`.station  AS station,
    `raw`.pollutant_id AS pollutant,
    CASE WHEN TRY_CAST(`raw`.avg_value AS DOUBLE) BETWEEN 0 AND 500
         THEN TRY_CAST(`raw`.avg_value AS DOUBLE) END AS idx,
    ts
FROM readings_raw
WHERE `raw`.station IS NOT NULL AND `raw`.pollutant_id IS NOT NULL AND ts IS NOT NULL;

-- ---------------------------------------------------------------- SINK
-- ---------------------------------------------------------------- JOB 4
-- STUCK SENSORS. An event-time OVER window with a RANGE frame: for each
-- reading, look back 6 hours within the same (station, pollutant). If there
-- are 5+ readings and MIN = MAX, every value was identical - the sensor is
-- frozen. Floor of 20: measured on 22k readings, 74% of "all identical" runs
-- were sub-indices under 20, where integer rounding of a low concentration
-- legitimately flatlines. Above 20 an unchanging value for 5 hours is a fault. Third streaming pattern in this project after continuous GROUP BY
-- and HOP: OVER windows emit one output row per INPUT row, enriched with an
-- aggregate over its own trailing history.
CREATE TABLE station_pollutant_stuck (
    station STRING, pollutant STRING, hour_ts TIMESTAMP(3),
    idx DOUBLE, readings_6h INT, stuck BOOLEAN,
    PRIMARY KEY (station, pollutant, hour_ts) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/airpulse',
    'table-name' = 'station_pollutant_stuck',
    'username' = 'airpulse', 'password' = 'airpulse'
);

INSERT INTO station_pollutant_stuck
SELECT station, pollutant, hour_ts, idx, readings_6h,
       readings_6h >= 5 AND min_6h = max_6h AND idx >= 20 AS stuck
FROM (
    SELECT
        station, pollutant, ts AS hour_ts, idx,
        CAST(COUNT(*) OVER w AS INT) AS readings_6h,
        MIN(idx) OVER w AS min_6h,
        MAX(idx) OVER w AS max_6h
    FROM readings
    WHERE idx IS NOT NULL
    WINDOW w AS (
        PARTITION BY station, pollutant
        ORDER BY ts
        RANGE BETWEEN INTERVAL '6' HOUR PRECEDING AND CURRENT ROW
    )
);
