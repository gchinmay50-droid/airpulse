-- GOLD layer: hour-over-hour jump flags. Written by Flink Job 3
-- (flink/sql/station_data_quality.sql).

-- One row per (station, pollutant, hour) reading, carrying the previous
-- reading it was compared against. A sub-index swing of >150 points in
-- <=2 hours (2-3 CPCB categories) is flagged as implausible - more likely a
-- sensor fault or a feed transcription error than real air chemistry.
CREATE TABLE station_pollutant_jumps (
    station        text             NOT NULL,
    pollutant      text             NOT NULL,
    hour_ts        timestamptz      NOT NULL,
    idx            double precision NOT NULL,
    prev_idx       double precision,
    prev_hour_ts   timestamptz,
    jump           double precision,          -- idx - prev_idx; NULL with no prior reading
    implausible    boolean          NOT NULL,
    updated_at     timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (station, pollutant, hour_ts)
);
CREATE INDEX station_pollutant_jumps_flagged_idx
    ON station_pollutant_jumps (hour_ts DESC) WHERE implausible;

-- Stuck-sensor detection. Written by Flink Job 4 (same file as Job 3).
-- One row per (station, pollutant, hour): how many readings in the trailing
-- 6 h window and whether they were ALL the same value. A real sub-index
-- moves; 5+ identical consecutive hourly values at a sub-index of 20 or more
-- means the sensor (or the feed slot) is frozen. Below 20, integer rounding
-- of a low concentration flatlines legitimately (measured: 74% of runs).
CREATE TABLE station_pollutant_stuck (
    station        text             NOT NULL,
    pollutant      text             NOT NULL,
    hour_ts        timestamptz      NOT NULL,
    idx            double precision NOT NULL,
    readings_6h    int              NOT NULL,   -- rows in the trailing 6 h window
    stuck          boolean          NOT NULL,   -- readings_6h >= 5 AND min = max AND idx >= 20
    updated_at     timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (station, pollutant, hour_ts)
);
CREATE INDEX station_pollutant_stuck_flagged_idx
    ON station_pollutant_stuck (hour_ts DESC) WHERE stuck;
