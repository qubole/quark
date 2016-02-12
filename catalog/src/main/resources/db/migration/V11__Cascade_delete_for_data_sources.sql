BEGIN;
alter table jdbc_sources drop foreign key `FK_JDBCSOURCE`;
alter table jdbc_sources add constraint `FK_JDBCSOURCE` foreign key(`id`) references `data_sources` (`id`) on delete cascade;

alter table quboledb_sources drop foreign key `FK_QUBOLEDBSOURCE`;
alter table quboledb_sources add constraint `FK_QUBOLEDBSOURCE` foreign key(`id`) references `data_sources` (`id`) on delete cascade;
COMMIT;