BEGIN;

drop table if exists `partitions`;
CREATE TABLE `partitions` (
    `id` INTEGER PRIMARY KEY auto_increment,
    `name` varchar(40) NOT NULL,
    `description` varchar(100),
    `cost` INTEGER, -- populate manually ?
    `query` text,
    `source_id` INT NOT NULL,
    `destination_id` INT NOT NULL,
    `schema_name` VARCHAR(40),
    `table_name` VARCHAR(40)
);

COMMIT;