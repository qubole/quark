alter table cubes add column grouping_column varchar(40);
alter table dimensions add column cube_column_name varchar(40);
alter table dimensions add column dimension_order int;
alter table measures add column cube_column_name varchar(40);