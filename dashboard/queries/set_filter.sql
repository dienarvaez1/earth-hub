-- Parameters, in order: start_time, end_time, min_magnitude, max_magnitude, country,
-- region, event_type, include_deleted, time_bucket, top_n, significant_magnitude.
UPDATE filter SET
    start_time = ?::TIMESTAMP,
    end_time = ?::TIMESTAMP,
    min_magnitude = ?::DOUBLE,
    max_magnitude = ?::DOUBLE,
    country = ?::VARCHAR,
    region = ?::VARCHAR,
    event_type = ?::VARCHAR,
    include_deleted = ?::BOOLEAN,
    time_bucket = ?::VARCHAR,
    top_n = ?::INTEGER,
    significant_magnitude = ?::DOUBLE;
