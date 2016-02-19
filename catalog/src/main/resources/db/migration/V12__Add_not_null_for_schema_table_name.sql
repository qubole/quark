BEGIN;

alter table `partitions` modify `schema_name` VARCHAR(40) not null;
alter table `partitions` modify `table_name` VARCHAR(40) not null;
alter table `cubes` modify `schema_name` VARCHAR(40) not null;
alter table `cubes` modify `table_name` VARCHAR(40) not null;

COMMIT;