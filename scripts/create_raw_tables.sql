USE smart_city;
SHOW VARIABLES LIKE 'secure_file_priv';

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