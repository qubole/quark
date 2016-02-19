BEGIN;

create table jdbc_sources (
    id int not null,
    username varchar(200) not null,
    password varchar(200) not null,
    primary key (`id`),
    constraint `FK_JDBCSOURCE` foreign key (`id`) references `data_sources` (`id`)
);

create table quboledb_sources (
    id int not null,
    dbtap_id int,
    auth_token varchar(200) not null,
    primary key (`id`),
    constraint `FK_QUBOLEDBSOURCE` foreign key (`id`) references `data_sources` (`id`)
);

--- Adding datasource type in datasource table
ALTER TABLE data_sources ADD COLUMN datasource_type VARCHAR(200) NOT NULL DEFAULT 'DataSource';

--- Adding defaultDataSource in DsSet table
ALTER TABLE ds_sets ADD COLUMN default_datasource_id int DEFAULT 0;
---ALTER TABLE ds_sets ADD constraint `FK_DEFAULT_DATASOURCE`
---FOREIGN KEY (`default_datasource``) REFERENCES `data_sources` (`id`) ON DELETE SET DEFAULT;

--- Dropping databasename not null constraints
ALTER TABLE data_sources ADD COLUMN temp_col VARCHAR(200) NOT NULL DEFAULT 'TMP';

UPDATE data_sources
SET temp_col = password;

ALTER TABLE data_sources DROP COLUMN password;

ALTER TABLE data_sources ADD COLUMN password VARCHAR(200);

UPDATE data_sources
SET password = temp_col;

ALTER TABLE data_sources DROP COLUMN temp_col;

--- for username
ALTER TABLE data_sources ADD COLUMN temp_col VARCHAR(200) NOT NULL DEFAULT 'TMP';

UPDATE data_sources
SET temp_col = username;

ALTER TABLE data_sources DROP COLUMN username;

ALTER TABLE data_sources ADD COLUMN username VARCHAR(200);

UPDATE data_sources
SET username = temp_col;

ALTER TABLE data_sources DROP COLUMN temp_col;

--- Databasename remove
ALTER TABLE data_sources ADD COLUMN temp_col VARCHAR(200) NOT NULL DEFAULT 'TMP';

UPDATE data_sources
SET temp_col = databasename;

ALTER TABLE data_sources DROP COLUMN databasename;

ALTER TABLE data_sources ADD COLUMN databasename VARCHAR(200);

UPDATE data_sources
SET databasename = temp_col;

ALTER TABLE data_sources DROP COLUMN temp_col;

--- Add ds_set_id to cubes and partitions
ALTER TABLE cubes ADD COLUMN ds_set_id int not null;
ALTER TABLE cubes add constraint `FK_CUBE_DSSETID` foreign key (`ds_set_id`) references `ds_sets`
    (`id`);

ALTER TABLE partitions ADD COLUMN ds_set_id int not null;
ALTER TABLE partitions add constraint `FK_PARTITION_DSSETID` foreign key (`ds_set_id`) references
`ds_sets`
    (`id`);

--- Dropping source_id
ALTER TABLE cubes DROP COLUMN source_id;
ALTER TABLE partitions DROP COLUMN source_id;

COMMIT;