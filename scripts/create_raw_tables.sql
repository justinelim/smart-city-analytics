USE smart_city;
SHOW VARIABLES LIKE 'secure_file_priv';

/* Streamed Offsets Table */
DROP TABLE IF EXISTS `streamed_offsets`;
CREATE TABLE `streamed_offsets` (
`dataset_name` VARCHAR(20) PRIMARY KEY,
`primary_key` VARCHAR(10),
`last_offset` VARCHAR(20)
);

/* Processed Offsets Table */
DROP TABLE IF EXISTS `processed_offsets`;
CREATE TABLE `processed_offsets` (
`dataset_name` VARCHAR(20) PRIMARY KEY,
`primary_key` VARCHAR(10),
`last_offset` VARCHAR(20)
);

/* Raw data */
DROP TABLE IF EXISTS `cultural_events`;
CREATE TABLE `cultural_events` (
`row_id` INT,
`city` VARCHAR(50),
`event_name` VARCHAR(255),
`ticket_url` VARCHAR(255),
`ticket_price` VARCHAR(50),
`timestamp` INT,
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

DROP TABLE IF EXISTS `library_events`;
CREATE TABLE `library_events` (
`lid` INT,
`city` VARCHAR(50),
`endtime` DATETIME,
`title` VARCHAR(255),
`url` VARCHAR(255),
`price` VARCHAR(255),
`changed` VARCHAR(255),
`content` TEXT,
`zipcode` VARCHAR(50),
`library` VARCHAR(255),
`imageurl` VARCHAR(255),
`teaser` TEXT,
`street` VARCHAR(255),
`status` VARCHAR(255),
`longitude` DECIMAL(12, 10),
`starttime` DATETIME,
`latitude` DECIMAL(12, 10),
`_id` VARCHAR(255),
`id` VARCHAR(255),
`streamtime` VARCHAR(255)
);

DROP TABLE IF EXISTS `parking`;
CREATE TABLE `parking` (
`vehiclecount` INT,
`updatetime` DATETIME,
`_id` INT,
`totalspaces` INT,
`garagecode` VARCHAR(255),
`streamtime` DATETIME
);

DROP TABLE IF EXISTS `parking_metadata`;
CREATE TABLE `parking_metadata` (
`garagecode` VARCHAR(255),
`city` VARCHAR(255),
`postalcode` INT,
`street` VARCHAR(255),
`housenumber` VARCHAR(50),
`latitude` DECIMAL(12, 10),
`longitude` DECIMAL(12, 10)
);

DROP TABLE IF EXISTS `pollution`;
CREATE TABLE `pollution` (
`ozone` INT,
`particullate_matter` INT,
`carbon_monoxide` INT,
`sulfure_dioxide` INT,
`nitrogen_dioxide` INT,
`longitude` DECIMAL(12, 10),
`latitude` DECIMAL(12, 10),
`timestamp` TIMESTAMP
);

ALTER TABLE pollution
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;

DROP TABLE IF EXISTS `road_traffic`;
CREATE TABLE `road_traffic` (
`status` CHAR(10),
`avgMeasuredTime` INT,
`avgSpeed` INT,
`extID` INT,
`medianMeasuredTime` INT,
`TIMESTAMP` TIMESTAMP,
`vehicleCount` INT,
`_id` BIGINT,
`REPORT_ID` BIGINT
);

DROP TABLE IF EXISTS `social_events`;
CREATE TABLE `social_events` (
`event_type` VARCHAR(255),
`webcast_url` VARCHAR(255),
`event_details` VARCHAR(255),
`webcast_url_alternate` VARCHAR(255),
`event_date` VARCHAR(255)
);

ALTER TABLE social_events
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;

DROP TABLE IF EXISTS `weather`;
CREATE TABLE `weather` (
`timestamp` TIMESTAMP,
`tempm` VARCHAR(255),
`wspdm` VARCHAR(255),
`dewptm` VARCHAR(255),
`hum` VARCHAR(255),
`pressurem` VARCHAR(255),
`vism` VARCHAR(255),
`wdird` VARCHAR(255)
);

/* Create indexing on primary keys to improve performance */
CREATE INDEX idx_event_id ON cultural_events (event_id);
CREATE INDEX idx_id ON library_events (id);
CREATE INDEX idx__id ON parking (_id);
CREATE INDEX idx_id ON pollution (id);
CREATE INDEX idx__id ON road_traffic (_id);
CREATE INDEX idx_id ON social_events (id);
CREATE INDEX idx_timestamp ON weather (timestamp);

