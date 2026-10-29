-- Every country, for the country filter, with its number of events under the rest of the
-- filter (time, magnitude, region, type, deletions, tsunami): the counts follow the other
-- choices.
SELECT country_name AS country,
       count(*) FILTER (WHERE in_time AND in_magnitude AND in_region AND in_type AND in_deleted
                              AND in_tsunami)
           AS events
FROM matched
GROUP BY country_name
ORDER BY country_name;
