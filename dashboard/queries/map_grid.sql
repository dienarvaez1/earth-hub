-- The map when it has too many events to draw one by one: the filtered events in cells of
-- map_view's cell_degrees, each at its events' mean position (to 4 decimals, about 11 m, so it
-- does not depend on the order the events are added in), with their number and strongest
-- magnitude. Strong events are left out: map_strong.sql gives them as their own points, so a
-- cell can never hide one, and the cells and strong events count every filtered event once.
SELECT
    floor(e.longitude / v.cell_degrees)::INTEGER AS cell_x,
    floor(e.latitude / v.cell_degrees)::INTEGER AS cell_y,
    round(avg(e.longitude), 4) AS longitude,
    round(avg(e.latitude), 4) AS latitude,
    count(*) AS events,
    max(e.magnitude) AS max_magnitude
FROM filtered AS e, map_view AS v
WHERE e.magnitude IS NULL OR e.magnitude < v.strong_magnitude
GROUP BY cell_x, cell_y
ORDER BY cell_x, cell_y;
