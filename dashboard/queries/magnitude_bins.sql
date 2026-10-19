-- Events per magnitude bin of 0.5: a bin holds bin_start <= magnitude < bin_end. Bins run
-- from the lowest to the highest one with an event; empty bins are listed with 0. Events
-- without a magnitude are counted in summary.sql, not here.
WITH counted AS (
    SELECT CAST(floor(magnitude * 2) AS INTEGER) AS k, count(*) AS events
    FROM filtered
    WHERE magnitude IS NOT NULL
    GROUP BY k
),
bins AS (
    SELECT unnest(generate_series(min(k), max(k))) AS k
    FROM counted
    HAVING count(*) > 0
)
SELECT b.k / 2.0 AS bin_start, (b.k + 1) / 2.0 AS bin_end, coalesce(c.events, 0) AS events
FROM bins AS b
LEFT JOIN counted AS c USING (k)
ORDER BY b.k;
