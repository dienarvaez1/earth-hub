-- The headline numbers for the filtered events (one row, also when there are none).
SELECT
    count(*) AS events,
    count(magnitude) AS with_magnitude,
    max(magnitude) AS max_magnitude,
    arg_max(place, magnitude) AS max_magnitude_place,
    min(event_ts) AS first_event,
    max(event_ts) AS last_event,
    count(DISTINCT country_name) AS countries,
    count(*) FILTER (WHERE tsunami) AS tsunami_flagged,
    count(*) FILTER (WHERE review_status = 'deleted') AS deleted
FROM filtered;
