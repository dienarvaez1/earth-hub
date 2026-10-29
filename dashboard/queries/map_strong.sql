-- With map_grid.sql: the events at or above map_view's strong_magnitude, each its own point,
-- as map_points.sql gives them.
SELECT e.key, e.longitude, e.latitude, e.depth_km, e.magnitude, e.event_time_ms, e.place,
       e.title, e.url, e.review_status
FROM filtered AS e, map_view AS v
WHERE e.magnitude >= v.strong_magnitude
ORDER BY e.key;
