-- Every event type with its number of events, most common first, for the type filter.
SELECT event_type, count(*) AS events
FROM events
GROUP BY event_type
ORDER BY events DESC, event_type;
