-- The first and last day with any event (deleted ones too): the custom calendar's limits.
SELECT min(event_ts) AS first_event, max(event_ts) AS last_event
FROM events;
