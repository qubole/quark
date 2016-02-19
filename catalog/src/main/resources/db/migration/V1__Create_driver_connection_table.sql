create table drivers (
    id int not null primary key auto_increment,
    name varchar(200) not null unique,
    type varchar(200) not null,
    url varchar(200) not null,
    databasename varchar(200) not null,
    username varchar(200) not null,
    password   varchar(200) not null
);