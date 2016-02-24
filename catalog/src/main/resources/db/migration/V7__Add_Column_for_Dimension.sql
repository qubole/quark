BEGIN;
alter table dimensions add column parent_dimension int null;
alter table dimensions add foreign key (`parent_dimension`) references `dimensions`(`id`) ON DELETE SET NULL;
COMMIT;