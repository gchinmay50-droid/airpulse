-- Flink's JDBC upsert only writes its own columns, so updated_at would keep
-- its INSERT-time value forever on later updates. This trigger makes it a
-- true "last modified" cursor, which src/publish_gold.py syncs on.
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END $$ LANGUAGE plpgsql;

ALTER TABLE station_completeness_24h ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

DO $$ DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['station_aqi_hourly','station_completeness_24h','station_pollutant_jumps','station_pollutant_stuck'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_touch ON %I', t, t);
    EXECUTE format('CREATE TRIGGER %I_touch BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION touch_updated_at()', t, t);
  END LOOP;
END $$;
