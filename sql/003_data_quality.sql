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
