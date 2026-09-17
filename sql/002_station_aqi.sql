-- GOLD layer: what the dashboard reads. Written by Flink (flink/sql/station_aqi.sql).

-- One row per (station, hour). Continuously UPSERTED by Flink as readings
-- arrive, so a late reading simply updates the row - no window ever "closes".
CREATE TABLE station_aqi_hourly (
    station          text        NOT NULL,
    hour_ts          timestamptz NOT NULL,   -- the measurement hour (UTC)
    city             text,
    state            text,
    latitude         double precision,
    longitude        double precision,
    pm25   double precision,  pm10  double precision,  no2 double precision,
    so2    double precision,  co    double precision,  o3  double precision,
    nh3    double precision,
    pollutants_reporting int NOT NULL,       -- how many of the 7 had a value
    aqi              int,                    -- NULL when CPCB's sufficiency rule fails
    dominant         text,                   -- pollutant with the worst sub-index
    category         text,                   -- Good / Satisfactory / Moderate / Poor / Very Poor / Severe
    insufficient_reason text,                -- why aqi is NULL, if it is
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (station, hour_ts)
);
CREATE INDEX station_aqi_hourly_ts_idx ON station_aqi_hourly (hour_ts DESC);

-- Trailing-24h reporting completeness per station, computed by an EVENT-TIME
-- hopping window in Flink (24h wide, sliding hourly). CPCB requires 16 of 24
-- hours for a valid AQI; the public feed never tells you whether that holds.
CREATE TABLE station_completeness_24h (
    station        text        NOT NULL,
    window_end     timestamptz NOT NULL,
    hours_reported int         NOT NULL,     -- distinct hours with >=1 valid sub-index
    meets_16h_rule boolean     NOT NULL,
    PRIMARY KEY (station, window_end)
);
