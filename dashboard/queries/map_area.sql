-- With map_grid.sql, once the map is zoomed in: the other events in map_view's area (west to
-- east, south to north, edges included; the whole world when west is NULL), each its own
-- point, as map_points.sql gives them. Longitudes are compared around the globe, so an area
-- crossing the antimeridian (east above 180) wraps. At most area_limit + 1 rows: more than
-- area_limit means the area still holds too many events to draw one by one.
SELECT e.key, e.longitude, e.latitude, e.depth_km, e.magnitude, e.event_time_ms, e.place,
       e.title, e.url, e.review_status, e.tsunami
FROM filtered AS e, map_view AS v
WHERE (e.magnitude IS NULL OR e.magnitude < v.strong_magnitude)
    AND (v.west IS NULL OR (
        ((e.longitude - v.west) % 360 + 360) % 360 <= v.east - v.west
        AND e.latitude BETWEEN v.south AND v.north
    ))
ORDER BY e.key
LIMIT (SELECT area_limit + 1 FROM map_view);
