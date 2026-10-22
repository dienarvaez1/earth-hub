-- The top_n countries by number of events; ties in alphabetical order. Each country's events
-- are also counted by magnitude level, as in over_time.sql; the levels add up to `events`.
SELECT country_name AS country, events, max_magnitude,
       below_2_5, from_2_5, from_4_5, from_6, no_magnitude
FROM (
    SELECT country_name, count(*) AS events, max(magnitude) AS max_magnitude,
           count(*) FILTER (WHERE magnitude < 2.5) AS below_2_5,
           count(*) FILTER (WHERE magnitude >= 2.5 AND magnitude < 4.5) AS from_2_5,
           count(*) FILTER (WHERE magnitude >= 4.5 AND magnitude < 6) AS from_4_5,
           count(*) FILTER (WHERE magnitude >= 6) AS from_6,
           count(*) FILTER (WHERE magnitude IS NULL) AS no_magnitude,
           row_number() OVER (ORDER BY count(*) DESC, country_name) AS rank
    FROM filtered
    GROUP BY country_name
)
WHERE rank <= (SELECT top_n FROM filter)
ORDER BY rank;
