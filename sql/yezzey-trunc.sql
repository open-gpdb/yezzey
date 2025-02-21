CREATE EXTENSION yezzey;
SET client_min_messages TO WARNING;

-- AO

CREATE TABLE trunc_regaoty(i INT) WITH (appendonly=true);
INSERT INTO trunc_regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('trunc_regaoty');

SELECT reltablespace FROM pg_class where oid = 'trunc_regaoty'::regclass::oid;

SELECT count() FROM yezzey_offload_relation_status('trunc_regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('trunc_regaoty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('trunc_regaoty');

SELECT count(1) FROM trunc_regaoty;
INSERT INTO trunc_regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM trunc_regaoty;

SELECT count() FROM yezzey_offload_relation_status('trunc_regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('trunc_regaoty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('trunc_regaoty');

DELETE FROM trunc_regaoty;
INSERT INTO trunc_regaoty SELECT * FROM generate_series(1, 100000);
INSERT INTO trunc_regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM trunc_regaoty;

TRUNCATE trunc_regaoty;
SELECT count(1) from yezzey_dump_virtual_index('trunc_regaoty');
SELECT count(1) FROM trunc_regaoty;

SELECT count() FROM yezzey_offload_relation_status('trunc_regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('trunc_regaoty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('trunc_regaoty');

INSERT INTO trunc_regaoty SELECT * FROM generate_series(1, 100000);
INSERT INTO trunc_regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM trunc_regaoty;

DROP TABLE trunc_regaoty;

\! echo AO simple test OK

-- AOCS

CREATE TABLE trunc_regaocsty(i INT) WITH (appendonly=true, orientation=column);
INSERT INTO trunc_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('trunc_regaocsty');

SELECT reltablespace FROM pg_class where oid = 'trunc_regaocsty'::regclass::oid;

SELECT count() FROM yezzey_offload_relation_status('trunc_regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('trunc_regaocsty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('trunc_regaocsty');

SELECT count(1) FROM trunc_regaocsty;
INSERT INTO trunc_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM trunc_regaocsty;

SELECT count() FROM yezzey_offload_relation_status('trunc_regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('trunc_regaocsty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('trunc_regaocsty');

DELETE FROM trunc_regaocsty;
INSERT INTO trunc_regaocsty SELECT * FROM generate_series(1, 100000);
INSERT INTO trunc_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM trunc_regaocsty;

TRUNCATE trunc_regaocsty;
SELECT count(1) from yezzey_dump_virtual_index('trunc_regaoty');
SELECT count(1) FROM trunc_regaocsty;

SELECT count() FROM yezzey_offload_relation_status('trunc_regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('trunc_regaocsty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('trunc_regaocsty');

INSERT INTO trunc_regaocsty SELECT * FROM generate_series(1, 100000);
INSERT INTO trunc_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM trunc_regaocsty;

DROP TABLE trunc_regaocsty;
\! echo AOCS simple test OK

DROP EXTENSION yezzey;
CHECKPOINT;
