USE smart_city;

SELECT * FROM streamed_offsets;
SELECT * FROM cleaned_offsets;
SELECT * FROM library_events WHERE id > 7827 ORDER BY id LIMIT 1;