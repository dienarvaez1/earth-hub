-- Events per hour, day, week (ISO, from Monday) or month, in UTC, from the first period with an
-- event to the last; periods without events are listed with 0. Each period's events are also
-- counted by magnitude level, the bands of the map's legend: below 2.5, 2.5 to 4.5, 4.5 to 6,
-- 6 and above (each band includes its lower edge), and no magnitude; the levels add up to
-- `events`.
WITH f AS (SELECT time_bucket FROM filter),
counted AS (
    SELECT date_trunc((SELECT time_bucket FROM f), event_ts) AS period,
           count(*) AS events,
           max(magnitude) AS max_magnitude,
           count(*) FILTER (WHERE magnitude < 2.5) AS below_2_5,
           count(*) FILTER (WHERE magnitude >= 2.5 AND magnitude < 4.5) AS from_2_5,
           count(*) FILTER (WHERE magnitude >= 4.5 AND magnitude < 6) AS from_4_5,
           count(*) FILTER (WHERE magnitude >= 6) AS from_6,
           count(*) FILTER (WHERE magnitude IS NULL) AS no_magnitude
    FROM filtered
    GROUP BY period
),
periods AS (
    SELECT unnest(generate_series(
        min(period),
        max(period),
        CASE (SELECT time_bucket FROM f)
            WHEN 'hour' THEN INTERVAL 1 HOUR
            WHEN 'day' THEN INTERVAL 1 DAY
            WHEN 'week' THEN INTERVAL 7 DAY
            ELSE INTERVAL 1 MONTH
        END
    )) AS period
    FROM counted
    HAVING count(*) > 0
)
SELECT p.period,
       coalesce(c.events, 0) AS events,
       c.max_magnitude,
       coalesce(c.below_2_5, 0) AS below_2_5,
       coalesce(c.from_2_5, 0) AS from_2_5,
       coalesce(c.from_4_5, 0) AS from_4_5,
       coalesce(c.from_6, 0) AS from_6,
       coalesce(c.no_magnitude, 0) AS no_magnitude
FROM periods AS p
LEFT JOIN counted AS c USING (period)
ORDER BY p.period;
