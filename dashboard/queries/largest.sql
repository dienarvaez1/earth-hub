-- The top_n strongest events at or above significant_magnitude, strongest first; equal
-- magnitudes newest first.
SELECT key, event_ts, magnitude, place, country_name AS country, depth_km, url
FROM (
    SELECT *, row_number() OVER (ORDER BY magnitude DESC, event_ts DESC, key DESC) AS rank
    FROM filtered
    WHERE magnitude >= (SELECT significant_magnitude FROM filter)
)
WHERE rank <= (SELECT top_n FROM filter)
ORDER BY rank;
