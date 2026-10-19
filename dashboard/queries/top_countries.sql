-- The top_n countries by number of events; ties in alphabetical order.
SELECT country_name AS country, events, max_magnitude
FROM (
    SELECT country_name, count(*) AS events, max(magnitude) AS max_magnitude,
           row_number() OVER (ORDER BY count(*) DESC, country_name) AS rank
    FROM filtered
    GROUP BY country_name
)
WHERE rank <= (SELECT top_n FROM filter)
ORDER BY rank;
