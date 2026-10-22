-- Every event type, for the type filter, with its number of events under the rest of the
-- filter (time, magnitude, country, region, deletions). Most common first over all the
-- events, so the order stays put while the counts follow the other choices.
SELECT event_type,
       count(*) FILTER (WHERE in_time AND in_magnitude AND in_country AND in_region AND in_deleted)
           AS events
FROM matched
GROUP BY event_type
ORDER BY count(*) DESC, event_type;
