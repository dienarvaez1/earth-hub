-- Run once, after the snapshot is attached as the view `snapshot`
-- (CREATE VIEW snapshot AS SELECT * FROM read_parquet('<the file or URL>')).
--
-- Times are bucketed in UTC from event_time_ms as a plain TIMESTAMP, so nothing depends on
-- the browser's time zone or on DuckDB's time zone support (the ICU extension).

-- The current filter: one row, changed only through set_filter.sql.
CREATE OR REPLACE TABLE filter (
    start_time TIMESTAMP,             -- inclusive; NULL: no lower bound
    end_time TIMESTAMP,               -- exclusive; NULL: no upper bound
    min_magnitude DOUBLE,             -- inclusive; a bound leaves out events without one
    max_magnitude DOUBLE,             -- inclusive
    country VARCHAR,                  -- as shown, so 'Unknown' selects events without one
    region VARCHAR,
    event_type VARCHAR,
    include_deleted BOOLEAN NOT NULL,
    time_bucket VARCHAR NOT NULL CHECK (time_bucket IN ('day', 'week', 'month')),
    top_n INTEGER NOT NULL CHECK (top_n > 0),
    significant_magnitude DOUBLE NOT NULL
);
INSERT INTO filter VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, 'month', 10, 4.5);

CREATE OR REPLACE VIEW events AS
SELECT
    *,
    make_timestamp(event_time_ms * 1000) AS event_ts,
    coalesce(country, 'Unknown') AS country_name,
    coalesce(region, 'Unknown') AS region_name
FROM snapshot;

CREATE OR REPLACE VIEW filtered AS
SELECT e.*
FROM events AS e, filter AS f
WHERE (f.start_time IS NULL OR e.event_ts >= f.start_time)
  AND (f.end_time IS NULL OR e.event_ts < f.end_time)
  AND (f.min_magnitude IS NULL OR e.magnitude >= f.min_magnitude)
  AND (f.max_magnitude IS NULL OR e.magnitude <= f.max_magnitude)
  AND (f.country IS NULL OR e.country_name = f.country)
  AND (f.region IS NULL OR e.region_name = f.region)
  AND (f.event_type IS NULL OR e.event_type = f.event_type)
  AND (f.include_deleted OR e.review_status <> 'deleted');
