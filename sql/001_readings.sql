-- One row per (station, pollutant, hour). This is the SILVER layer:
-- validated, typed, flagged - but never silently dropped.

CREATE TABLE readings (
    station       text              NOT NULL,
    pollutant     text              NOT NULL,   -- PM2.5, PM10, NO2, SO2, CO, OZONE, NH3
    event_time    timestamptz       NOT NULL,   -- when the sensor measured (UTC)
    city          text,
    state         text,
    latitude      double precision,
    longitude     double precision,
    sub_index_min     double precision,             -- NULL when the source said NA or the value failed validation
    sub_index_max     double precision,
    sub_index_avg     double precision,
    quality_flags text[]            NOT NULL DEFAULT '{}',  -- e.g. {NA} or {NEGATIVE,MIN_GT_MAX}
    ingested_at   timestamptz       NOT NULL,   -- when the producer published it
    processed_at  timestamptz       NOT NULL DEFAULT now(),

    -- The fingerprint IS the primary key. Inserting a duplicate is a no-op
    -- (ON CONFLICT DO NOTHING), which makes the sink idempotent: Kafka may
    -- redeliver a message after a crash, and nothing bad happens.
    PRIMARY KEY (station, pollutant, event_time)
);

-- The two access patterns the dashboard will hit hardest.
CREATE INDEX readings_event_time_idx ON readings (event_time DESC);
CREATE INDEX readings_station_time_idx ON readings (station, event_time DESC);
