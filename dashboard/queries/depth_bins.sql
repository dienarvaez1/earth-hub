-- The depth-against-magnitude chart when the map has too many events to draw one by one:
-- events counted in bins of 0.1 magnitude by 5 km of depth, each bin named by its lower edges
-- (`magnitude_from <= magnitude < magnitude_from + 0.1`, the same for depth). Events without
-- a magnitude are left out, as the chart leaves them out of its points.
SELECT
    floor(round(magnitude * 10, 6)) / 10 AS magnitude_from,
    floor(depth_km / 5) * 5 AS depth_from_km,
    count(*) AS events
FROM filtered
WHERE magnitude IS NOT NULL
GROUP BY magnitude_from, depth_from_km
ORDER BY magnitude_from, depth_from_km;
