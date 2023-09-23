SELECT table_schema AS 'Database', 
       table_name AS 'Table', 
       ROUND((data_length + index_length) / (1024 * 1024), 2) AS 'Size (MB)'
FROM information_schema.tables
WHERE table_schema = 'smart_city';

SET @sql = NULL;
SELECT GROUP_CONCAT(
    CONCAT('SELECT ''', table_name, ''' AS table_name, COUNT(*) AS count FROM ', table_name)
    SEPARATOR ' UNION ALL '
) INTO @sql
FROM information_schema.tables
WHERE table_schema = 'smart_city';

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


