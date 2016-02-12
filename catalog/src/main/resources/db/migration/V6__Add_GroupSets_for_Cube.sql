drop table if exists `groups`;
create table `groups` (
`id` INTEGER NOT NULL,
`dimension_id` INTEGER NOT NULL,
PRIMARY KEY (`id`, `dimension_id`),
FOREIGN KEY (`dimension_id`) REFERENCES `dimensions`(`id`)
ON DELETE CASCADE
);