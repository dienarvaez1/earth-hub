-- Events per day, week (ISO, from Monday) or month, in UTC, from the first period with an
-- event to the last; periods without events are listed with 0.
WITH f AS (SELECT time_bucket FROM filter),
counted AS (
    SELECT date_trunc((SELECT time_bucket FROM f), event_ts) AS period,
           count(*) AS events,
           max(magnitude) AS max_magnitude
    FROM filtered
    GROUP BY period
),
periods AS (
    SELECT unnest(generate_series(
        min(period),
        max(period),
        CASE (SELECT time_bucket FROM f)
            WHEN 'day' THEN INTERVAL 1 DAY
            WHEN 'week' THEN INTERVAL 7 DAY
            ELSE INTERVAL 1 MONTH
        END
    )) AS period
    FROM counted
    HAVING count(*) > 0
)
SELECT p.period, coalesce(c.events, 0) AS events, c.max_magnitude
FROM periods AS p
LEFT JOIN counted AS c USING (period)
ORDER BY p.period;
