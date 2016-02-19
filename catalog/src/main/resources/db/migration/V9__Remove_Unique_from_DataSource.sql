BEGIN;
ALTER TABLE data_sources ADD COLUMN temp_col VARCHAR(200) NOT NULL DEFAULT 'TMP';

UPDATE data_sources
SET temp_col = name;

ALTER TABLE data_sources DROP COLUMN name;

ALTER TABLE data_sources ADD COLUMN name VARCHAR(200) NOT NULL;

UPDATE data_sources
SET name = temp_col;

ALTER TABLE data_sources DROP COLUMN temp_col;
COMMIT;
