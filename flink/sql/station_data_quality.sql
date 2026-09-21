-- AirPulse Stage B (cont'd): per-pollutant hour-over-hour jump detection.
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
    'properties.group.id' = 'airpulse-flink-data-quality',
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
CREATE TABLE station_pollutant_jumps (
    station STRING, pollutant STRING, hour_ts TIMESTAMP(3),
    idx DOUBLE, prev_idx DOUBLE, prev_hour_ts TIMESTAMP(3),
    jump DOUBLE, implausible BOOLEAN,
    PRIMARY KEY (station, pollutant, hour_ts) NOT ENFORCED   -- makes the JDBC sink UPSERT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/airpulse',
    'table-name' = 'station_pollutant_jumps',
    'username' = 'airpulse', 'password' = 'airpulse',
    -- Benchmarked: 100-row default flush capped throughput at ~7k msg/s; 2000 rows -> 35k msg/s at parallelism 4.
    'sink.buffer-flush.max-rows' = '2000', 'sink.buffer-flush.interval' = '2 s'
);

-- ---------------------------------------------------------------- JOB 3
-- Continuous, like Job 1: LAG per (station, pollutant) over the raw reading
-- stream, ordered by event time. Flags a swing too large for real air
-- chemistry to produce within a couple of hours - more likely a sensor fault
-- or a transcription error in the feed. Readings more than 2h apart are
-- never compared: a station coming back online after a dark spell is a
-- different phenomenon, not a "jump".
INSERT INTO station_pollutant_jumps
SELECT
    -- Only one column can carry the rowtime attribute into the sink; strip it
    -- from prev_hour_ts with a cast since hour_ts is the one that matters.
    station, pollutant, hour_ts, idx, prev_idx, CAST(prev_hour_ts AS TIMESTAMP(3)) AS prev_hour_ts,
    idx - prev_idx AS jump,
    prev_idx IS NOT NULL
        AND TIMESTAMPDIFF(MINUTE, prev_hour_ts, hour_ts) <= 120
        AND ABS(idx - prev_idx) > 150 AS implausible
FROM (
    SELECT
        station, pollutant, ts AS hour_ts, idx,
        LAG(idx) OVER (PARTITION BY station, pollutant ORDER BY ts) AS prev_idx,
        LAG(ts)  OVER (PARTITION BY station, pollutant ORDER BY ts) AS prev_hour_ts
    FROM readings
    WHERE idx IS NOT NULL
);

