create table ds_sets (
  id int not null primary key auto_increment,
  name varchar(200) not null unique
);

alter table drivers rename to data_sources;

alter table data_sources add column ds_set_id int not null;

alter table data_sources add column is_default int default 0;