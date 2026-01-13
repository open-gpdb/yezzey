CREATE EXTENSION yezzey;
ALTER EXTENSION yezzey UPDATE TO '1.8.6';
CREATE TABLE vacuum_garbage_aot(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
select yezzey_define_offload_policy('vacuum_garbage_aot');
INSERT INTO vacuum_garbage_aot VALUES(1);
DELETE FROM vacuum_garbage_aot;
INSERT INTO vacuum_garbage_aot VALUES(1);
-- should be two files, old and new
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot');
VACUUM vacuum_garbage_aot;
SELECT yezzey_vacuum_garbage(true, true);
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot');

DROP TABLE vacuum_garbage_aot;

-- should be zero
SELECT yezzey_vacuum_garbage(true, true);
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot');

DROP EXTENSION yezzey;
CHECKPOINT;
