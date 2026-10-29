-- The significant_n strongest events at or above significant_magnitude, strongest first; equal
-- magnitudes newest first.
-- With what the map's popup shows, since a row opens it (map_points.sql's columns).
SELECT key, event_ts, magnitude, place, country_name AS country, depth_km, url,
       longitude, latitude, event_time_ms, title, review_status, tsunami
FROM (
    SELECT *, row_number() OVER (ORDER BY magnitude DESC, event_ts DESC, key DESC) AS rank
    FROM filtered
    WHERE magnitude >= (SELECT significant_magnitude FROM filter)
)
WHERE rank <= (SELECT significant_n FROM filter)
ORDER BY rank;
