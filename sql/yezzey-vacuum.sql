
CREATE EXTENSION yezzey VERSION '1.0';
CREATE TABLE vacuum_aot(i INT) WITH (appendonly=true);
select yezzey_define_offload_policy('vacuum_aot');
insert into vacuum_aot select * from generate_series(1, 10000);
insert into vacuum_aot select * from generate_series(1, 10000);
delete from vacuum_aot;
insert into vacuum_aot select * from generate_series(10000, 20000);
VACUUM vacuum_aot;
select count(1) from vacuum_aot; -- works ok, use second block(segment) of relation ;
insert into vacuum_aot select * from generate_series(10000, 20000); -- insert goes in first block(segment) with modcount 4
select count(1) from vacuum_aot; -- works

DROP TABLE vacuum_aot;

DROP EXTENSION yezzey;
CHECKPOINT;
