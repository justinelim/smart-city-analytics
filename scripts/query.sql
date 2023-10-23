USE smart_city;

-- SELECT COUNT(DISTINCT organizer_id) FROM cultural_events;
-- WHERE event_escription NOT LIKE '<p>%';
SELECT * FROM cultural_events
WHERE organizer_id = 334;
SELECT DISTINCT event_type FROM cultural_events
WHERE event_type LIKE 'Rock%';

SELECT * FROM library_events
WHERE id = 8608;
SELECT * FROM library_events;
SELECT COUNT(*) FROM library_events;
SELECT COUNT(distinct id) FROM library_events;

SELECT * FROM cultural_events;
SELECT * FROM library_events;
SELECT * FROM parking;
SELECT * FROM parking_metadata;
SELECT count(*) FROM pollution;
SELECT * FROM road_traffic;
SELECT * FROM social_events;
SELECT * FROM weather;

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
