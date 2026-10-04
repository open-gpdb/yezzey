CREATE EXTENSION yezzey VERSION '1.0';
ALTER EXTENSION yezzey UPDATE TO '1.8.6';

CREATE TABLE vacuum_garbage_aot(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
SELECT yezzey.offload_relation('vacuum_garbage_aot');

CREATE TABLE vacuum_garbage_aot_r(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
SELECT yezzey.offload_relation('vacuum_garbage_aot_r');

-- check how it work with ONLY given relation
INSERT INTO vacuum_garbage_aot_r VALUES(1);
DELETE FROM vacuum_garbage_aot_r;
INSERT INTO vacuum_garbage_aot_r VALUES(1);
VACUUM vacuum_garbage_aot_r;

INSERT INTO vacuum_garbage_aot VALUES(1);
DELETE FROM vacuum_garbage_aot;
INSERT INTO vacuum_garbage_aot VALUES(1);
VACUUM vacuum_garbage_aot;

-- should be three files, old and new, and after vacuum
SELECT count(1) FROM yezzey.relation_describe_external_storage_structure('vacuum_garbage_aot_r');

-- should be three files, old and new, and after vacuum
SELECT count(1) FROM yezzey.relation_describe_external_storage_structure('vacuum_garbage_aot');

SELECT yezzey_vacuum_garbage_relation('vacuum_garbage_aot_r', true, true);

-- should single file.
SELECT count(1) FROM yezzey.relation_describe_external_storage_structure('vacuum_garbage_aot_r');

-- should not change
SELECT count(1) FROM yezzey.relation_describe_external_storage_structure('vacuum_garbage_aot');

-- should be one file in each relation
SELECT yezzey.vacuum(true, true);
SELECT count(1) FROM yezzey.relation_describe_external_storage_structure('vacuum_garbage_aot');
SELECT count(1) FROM yezzey.relation_describe_external_storage_structure('vacuum_garbage_aot_r');

DROP TABLE vacuum_garbage_aot;
DROP TABLE vacuum_garbage_aot_r;

-- should be zero
SELECT yezzey.vacuum(true, true);
SELECT count(1) FROM yezzey.relation_describe_external_storage_structure('vacuum_garbage_aot');
SELECT count(1) FROM yezzey.relation_describe_external_storage_structure('vacuum_garbage_aot_r');

DROP EXTENSION yezzey;
CHECKPOINT;
