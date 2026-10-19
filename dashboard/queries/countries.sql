-- Every country with its number of events, for the country filter (not itself filtered).
SELECT country_name AS country, count(*) AS events
FROM events
GROUP BY country_name
ORDER BY country_name;
