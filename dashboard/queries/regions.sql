-- Every region in the selected country (all regions when none is selected), for the
-- region filter, with its number of events under the rest of the filter (time, magnitude,
-- type, deletions).
SELECT region_name AS region,
       count(*) FILTER (WHERE in_time AND in_magnitude AND in_type AND in_deleted) AS events
FROM matched
WHERE in_country
GROUP BY region_name
ORDER BY region_name;
