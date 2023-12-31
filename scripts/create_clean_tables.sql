USE smart_city;
SHOW VARIABLES LIKE 'secure_file_priv';

/* Clean data */
DROP TABLE IF EXISTS `clean_cultural_events`;
CREATE TABLE `clean_cultural_events` (
`row_id` INT,
`city` VARCHAR(50),
`event_name` VARCHAR(255),
`ticket_url` VARCHAR(255),
`avg_ticket_price` DECIMAL(7, 2),
`timestamp` DATETIME,
`postal_code` INT,
`longitude` DECIMAL(12, 10),
`event_id` INT,
`event_description` TEXT,
`venue_address` VARCHAR(255),
`venue_name` VARCHAR(255),
`event_date` TIMESTAMP,
`latitude` DECIMAL(12, 10),
`venue_url` VARCHAR(255),
`organizer_id` INT,
`category` VARCHAR(255),
`image_url` VARCHAR(255),
`event_type` VARCHAR(50)
);

DROP TABLE IF EXISTS `clean_library_events`;
CREATE TABLE `clean_library_events` (
`library_id` INT,
`city` VARCHAR(50),
`end_time` DATETIME,
`title` VARCHAR(255),
`url` VARCHAR(255),
`price` VARCHAR(255),
`changed` VARCHAR(255),
`content` TEXT,
`zipcode` VARCHAR(50),
`library` VARCHAR(255),
`image_url` VARCHAR(255),
`teaser` TEXT,
`street` VARCHAR(255),
`status` VARCHAR(255),
`longitude` DECIMAL(12, 10),
`start_time` DATETIME,
`latitude` DECIMAL(12, 10),
`_id` VARCHAR(255),
`id` VARCHAR(255),
`stream_time` VARCHAR(255)
);

DROP TABLE IF EXISTS `clean_parking`;
CREATE TABLE `clean_parking` (
`vehicle_count` INT,
`update_time` DATETIME,
`_id` INT,
`total_spaces` INT,
`garage_code` VARCHAR(255),
`stream_time` DATETIME,
`city` VARCHAR(255),
`postal_code` INT,
`street` VARCHAR(255),
`house_number` VARCHAR(50),
`latitude` DECIMAL(12, 10),
`longitude` DECIMAL(12, 10)
);

DROP TABLE IF EXISTS `clean_pollution`;
CREATE TABLE `clean_pollution` (
`ozone` INT,
`particulate_matter` INT,
`carbon_monoxide` INT,
`sulfur_dioxide` INT,
`nitrogen_dioxide` INT,
`longitude` DECIMAL(12, 10),
`latitude` DECIMAL(12, 10),
`timestamp` TIMESTAMP
);

DROP TABLE IF EXISTS `clean_road_traffic`;
CREATE TABLE `clean_road_traffic` (
`status` CHAR(10),
`avg_measured_time` INT,
`avg_speed` INT,
`ext_id` INT,
`median_measured_time` INT,
`timestamp` TIMESTAMP,
`vehicle_count` INT,
`_id` BIGINT,
`report_id` BIGINT
);

DROP TABLE IF EXISTS `clean_social_events`;
CREATE TABLE `clean_social_events` (
`event_type` VARCHAR(255),
`webcast_url` VARCHAR(255),
`event_details` VARCHAR(255),
`webcast_url_alternate` VARCHAR(255),
`event_date` VARCHAR(255)
);

DROP TABLE IF EXISTS `clean_weather`;
CREATE TABLE `clean_weather` (
`timestamp` TIMESTAMP,
`temperature` VARCHAR(255),  -- degrees Celsius
`wind_speed` VARCHAR(255),  -- kilometers per hour (kph)
`dewptm` VARCHAR(255),  -- degrees Celsius
`humidity` VARCHAR(255),  -- %
`pressure` VARCHAR(255),  -- mBar
`visibility` VARCHAR(255),  -- km
`wind_direction` VARCHAR(255)  -- degrees
);