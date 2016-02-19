BEGIN;

drop table if exists `cubes`;
CREATE TABLE `cubes` (
    `id` INTEGER PRIMARY KEY auto_increment,
    `name` varchar(40) NOT NULL,
    `description` varchar(100),
    `cost` INTEGER, -- populate manually ?
    `query` text,
    `source_id` INT NOT NULL,
    `destination_id` INT NOT NULL
);

drop table if exists `dimensions`;
CREATE TABLE `dimensions` (
    `id` INTEGER PRIMARY KEY auto_increment,
    `name` varchar(40) NOT NULL, -- default name in the fact_table
    `cube_id` INTEGER NOT NULL,
    `schema_name` varchar(40),
    `table_name` varchar(40), -- mention the table_name or join_id if use_fact_table is false
    `column_name` varchar(40) NOT NULL
);

-- drop table if exists `dimension_hierarchies`;
-- CREATE TABLE `dimension_hierarchies` (
-- `id` INTEGER PRIMARY KEY,
-- `dimension_id` INTEGER  NOT NULL,
-- `table_name` varchar(40), -- mention the table_name or join_id if use_fact_table is false
-- `join_on_column_name` varchar(40) NOT NULL,
-- `join_type` varchar(40) NOT NULL, -- join between fact_table & dimension table - inner/left/right - move these columns to joins ?
-- `join_id` INTEGER, -- mention the table_name or join_id if use_fact_table is false
-- `use_fact_table` tinyint NOT NULL, -- if this is true => it will be assumed to be fact_table,also dimensions.foreign_key needs to be empty
-- `has_all` tinyint NOT NULL, -- will the dimension have a default level of "ALL" -> grand_total
-- `all_member_name` varchar(40), --the human readable name for the "ALL" / "GRAND_TOTAL" member name - this will default to "total"
-- `created_at` DATETIME DEFAULT NULL,
-- `updated_at` DATETIME DEFAULT NULL
-- );

-- drop table if exists `dimension_hierarchy_levels`;
-- CREATE TABLE `dimension_hierarchy_levels` (
--  `id` INTEGER PRIMARY KEY,
--  `dimension_id` INTEGER  NOT NULL,
--  `name` varchar(40) NOT NULL, -- human readable name
--  `column_name` varchar(40) NOT NULL, -- the column name being used in the dimension_hierarchies -> table_name
--  `ordinal_priority` INTEGER NOT NULL, -- the order of levels
--  `parent_dimension_hierarchy_level_id` INTEGER, -- the parent level
--  `created_at` DATETIME DEFAULT NULL,
--  `updated_at` DATETIME DEFAULT NULL
-- );

drop table if exists `measures`;
CREATE TABLE `measures` (
  `id` INTEGER PRIMARY KEY auto_increment,
  `name` varchar(40) NOT NULL, -- human readable name for the measure
  `cube_id` INTEGER NOT NULL,
  `column_name` varchar(40), -- the column-name identifiers being used for the measure
  `function` varchar(40) NOT NULL -- e.g. sum,min,max
);
COMMIT;