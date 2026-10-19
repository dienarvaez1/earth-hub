-- The top_n regions (with their country) by number of events; ties in alphabetical order.
SELECT region_name AS region, country_name AS country, events, max_magnitude
FROM (
    SELECT region_name, country_name, count(*) AS events, max(magnitude) AS max_magnitude,
           row_number() OVER (ORDER BY count(*) DESC, region_name, country_name) AS rank
    FROM filtered
    GROUP BY region_name, country_name
)
WHERE rank <= (SELECT top_n FROM filter)
ORDER BY rank;
