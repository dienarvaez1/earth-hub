-- Every region in the selected country (all regions when none is selected), for the
-- region filter.
SELECT region_name AS region, count(*) AS events
FROM events, filter AS f
WHERE f.country IS NULL OR events.country_name = f.country
GROUP BY region_name
ORDER BY region_name;
