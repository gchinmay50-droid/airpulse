-- AirPulse Stage B: per-station AQI, computed by Apache Flink from the raw topic.
-- Submit:  docker exec airpulse-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/station_aqi.sql

SET 'execution.checkpointing.interval' = '60 s';
SET 'table.local-time-zone' = 'UTC';
-- Kafka partitions with no traffic would otherwise hold the watermark back forever.
SET 'table.exec.source.idle-timeout' = '5 min';
-- Continuous aggregations keep state per key; expire keys nobody has touched in 2 days.
SET 'table.exec.state.ttl' = '48 h';

-- ---------------------------------------------------------------- SOURCE
CREATE TABLE readings_raw (
    schema_version INT,
    `source`       STRING,
    ingested_at    STRING,
    event_time     STRING,
    raw ROW<
        station STRING, pollutant_id STRING, city STRING, `state` STRING,
        latitude STRING, longitude STRING,
        min_value STRING, max_value STRING, avg_value STRING, last_update STRING
    >,
    -- event_time is ISO-8601 UTC, e.g. 2026-09-15T18:30:00+00:00.
    -- Take the first 19 chars; with local-time-zone=UTC this is exact.
    ts AS TO_TIMESTAMP(SUBSTR(event_time, 1, 19), 'yyyy-MM-dd''T''HH:mm:ss'),
    -- WATERMARK: "I have probably seen everything up to ts - 30 min".
    -- CPCB publishes an hour ~1h late and pagination leaks are caught within
    -- one 10-minute poll, so 30 min of lateness tolerance is comfortable.
    WATERMARK FOR ts AS ts - INTERVAL '30' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'aqi.readings.raw',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'airpulse-flink-station-aqi',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'     -- smoke-test messages must not kill the job
);

-- A typed, cleaned view. "NA" and junk become NULL; index must be in [0,500].
CREATE TEMPORARY VIEW readings AS
SELECT
    raw.station  AS station,
    raw.pollutant_id AS pollutant,
    raw.city AS city, raw.`state` AS `state`,
    TRY_CAST(raw.latitude  AS DOUBLE) AS latitude,
    TRY_CAST(raw.longitude AS DOUBLE) AS longitude,
    CASE WHEN TRY_CAST(raw.avg_value AS DOUBLE) BETWEEN 0 AND 500
         THEN TRY_CAST(raw.avg_value AS DOUBLE) END AS idx,
    ts
FROM readings_raw
WHERE raw.station IS NOT NULL AND raw.pollutant_id IS NOT NULL AND ts IS NOT NULL;

-- ---------------------------------------------------------------- SINKS
CREATE TABLE station_aqi_hourly (
    station STRING, hour_ts TIMESTAMP(3), city STRING, `state` STRING,
    latitude DOUBLE, longitude DOUBLE,
    pm25 DOUBLE, pm10 DOUBLE, no2 DOUBLE, so2 DOUBLE, co DOUBLE, o3 DOUBLE, nh3 DOUBLE,
    pollutants_reporting INT, aqi INT, dominant STRING, category STRING,
    insufficient_reason STRING,
    PRIMARY KEY (station, hour_ts) NOT ENFORCED   -- makes the JDBC sink UPSERT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/airpulse',
    'table-name' = 'station_aqi_hourly',
    'username' = 'airpulse', 'password' = 'airpulse'
);

CREATE TABLE station_completeness_24h (
    station STRING, window_end TIMESTAMP(3), hours_reported INT, meets_16h_rule BOOLEAN,
    PRIMARY KEY (station, window_end) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/airpulse',
    'table-name' = 'station_completeness_24h',
    'username' = 'airpulse', 'password' = 'airpulse'
);

-- ---------------------------------------------------------------- JOB 1
-- Continuous aggregation: GROUP BY with no window. Flink emits an updated
-- row every time a new reading for (station, hour) arrives, and the upsert
-- sink overwrites. Low latency; late data is just another update.
INSERT INTO station_aqi_hourly
SELECT
    station, hour_ts, city, `state`, latitude, longitude,
    pm25, pm10, no2, so2, co, o3, nh3,
    pollutants_reporting,
    CASE WHEN sufficient THEN CAST(worst AS INT) END AS aqi,
    CASE WHEN NOT sufficient THEN CAST(NULL AS STRING)
         WHEN worst = pm25 THEN 'PM2.5' WHEN worst = pm10 THEN 'PM10'
         WHEN worst = o3   THEN 'OZONE' WHEN worst = no2  THEN 'NO2'
         WHEN worst = so2  THEN 'SO2'   WHEN worst = co   THEN 'CO'
         WHEN worst = nh3  THEN 'NH3' END AS dominant,
    CASE WHEN NOT sufficient THEN CAST(NULL AS STRING)
         WHEN worst <= 50  THEN 'Good'        WHEN worst <= 100 THEN 'Satisfactory'
         WHEN worst <= 200 THEN 'Moderate'    WHEN worst <= 300 THEN 'Poor'
         WHEN worst <= 400 THEN 'Very Poor'   ELSE 'Severe' END AS category,
    CASE WHEN sufficient THEN CAST(NULL AS STRING)
         WHEN pollutants_reporting < 3 THEN 'fewer than 3 pollutants'
         ELSE 'neither PM2.5 nor PM10 available' END AS insufficient_reason
FROM (
    SELECT *,
        -- CPCB National AQI rules: >=3 pollutants, one of them PM, worst wins.
        (pollutants_reporting >= 3 AND (pm25 IS NOT NULL OR pm10 IS NOT NULL)) AS sufficient,
        GREATEST(COALESCE(pm25,-1), COALESCE(pm10,-1), COALESCE(no2,-1), COALESCE(so2,-1),
                 COALESCE(co,-1),   COALESCE(o3,-1),   COALESCE(nh3,-1)) AS worst
    FROM (
        SELECT
            station, ts AS hour_ts,
            MAX(city) AS city, MAX(`state`) AS `state`,
            MAX(latitude) AS latitude, MAX(longitude) AS longitude,
            MAX(CASE WHEN pollutant = 'PM2.5' THEN idx END) AS pm25,
            MAX(CASE WHEN pollutant = 'PM10'  THEN idx END) AS pm10,
            MAX(CASE WHEN pollutant = 'NO2'   THEN idx END) AS no2,
            MAX(CASE WHEN pollutant = 'SO2'   THEN idx END) AS so2,
            MAX(CASE WHEN pollutant = 'CO'    THEN idx END) AS co,
            MAX(CASE WHEN pollutant = 'OZONE' THEN idx END) AS o3,
            MAX(CASE WHEN pollutant = 'NH3'   THEN idx END) AS nh3,
            CAST(COUNT(DISTINCT CASE WHEN idx IS NOT NULL THEN pollutant END) AS INT) AS pollutants_reporting
        FROM readings
        GROUP BY station, ts
    )
);

-- ---------------------------------------------------------------- JOB 2
-- EVENT-TIME HOPPING WINDOW: 24h wide, slides every hour. A window only emits
-- once the WATERMARK passes its end - that is the guarantee that late data
-- (up to 30 min) has been waited for. This is the classic streaming pattern.
INSERT INTO station_completeness_24h
SELECT
    station,
    window_end,
    CAST(COUNT(DISTINCT ts) AS INT) AS hours_reported,
    COUNT(DISTINCT ts) >= 16      AS meets_16h_rule
FROM TABLE(
    HOP(TABLE readings, DESCRIPTOR(ts), INTERVAL '1' HOUR, INTERVAL '24' HOUR)
)
WHERE idx IS NOT NULL
GROUP BY station, window_start, window_end;
