-- One point per filtered event, for the map and the depth-against-magnitude chart.
SELECT id, longitude, latitude, depth_km, magnitude, event_time_ms, place, title, url,
       review_status
FROM filtered
ORDER BY id;
