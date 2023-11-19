USE smart_city;

-- SELECT COUNT(DISTINCT organizer_id) FROM cultural_events;
-- WHERE event_escription NOT LIKE '<p>%';

SELECT * FROM cultural_events ORDER BY event_id;
-- WHERE organizer_id = 334; 
SELECT DISTINCT event_type FROM cultural_events
WHERE event_type LIKE 'Rock%';

SELECT * FROM library_events
WHERE id = 8608;
SELECT * FROM library_events;
SELECT COUNT(*) FROM library_events;
SELECT COUNT(distinct id) FROM library_events;

SELECT * FROM streamed_offsets;
SELECT * FROM processed_offsets;

SELECT * FROM cultural_events; -- row count: 100
SELECT * FROM library_events; -- row count: 1548
SELECT * FROM parking; -- row count: 55264
SELECT * FROM parking_metadata; -- row count: 8
SELECT * FROM pollution; -- row count: 15775948
SELECT * FROM road_traffic; -- row count: 11520409
SELECT * FROM social_events; -- row count: 30
SELECT * FROM weather; -- row count: 12579

SELECT * FROM clean_cultural_events;
SELECT * FROM clean_library_events;
SELECT * FROM clean_parking;
SELECT * FROM clean_pollution;
SELECT * FROM clean_road_traffic;
SELECT * FROM clean_social_events;
SELECT * FROM clean_weather;

-- TRUNCATE smart_city.cultural_events;
-- TRUNCATE smart_city.weather;
-- TRUNCATE smart_city.parking_metadata;
TRUNCATE smart_city.clean_parking;

-- Identifying primary keys
SELECT * FROM weather
WHERE timestamp IS NULL;

SELECT timestamp, COUNT(*)
FROM weather
GROUP BY timestamp
HAVING COUNT(*) > 1;

SHOW VARIABLES LIKE 'wait_timeout';


