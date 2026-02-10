CREATE EXTENSION yezzey VERSION '1.0';
SET client_min_messages TO WARNING;
-- AO

CREATE TABLE alter_regaoty(i INT) WITH (appendonly=true);
INSERT INTO alter_regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('alter_regaoty');

SELECT reltablespace FROM pg_class where oid = 'alter_regaoty'::regclass::oid;

SELECT count() FROM yezzey_offload_relation_status('alter_regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('alter_regaoty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('alter_regaoty');

SELECT count(1) FROM alter_regaoty;
INSERT INTO alter_regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM alter_regaoty;

SELECT count() FROM yezzey_offload_relation_status('alter_regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('alter_regaoty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('alter_regaoty');

DELETE FROM alter_regaoty;
INSERT INTO alter_regaoty SELECT * FROM generate_series(1, 100000);
INSERT INTO alter_regaoty SELECT * FROM generate_series(1, 100000);

SELECT count(1) FROM alter_regaoty;

ALTER TABLE alter_regaoty SET DISTRIBUTED RANDOMLY;
SELECT count(1) FROM alter_regaoty;

SELECT count() FROM yezzey_offload_relation_status('alter_regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('alter_regaoty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('alter_regaoty');

DROP TABLE alter_regaoty;

\! echo AO simple test OK

-- AOCS

CREATE TABLE alter_regaocsty(i INT) WITH (appendonly=true, orientation=column);
INSERT INTO alter_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('alter_regaocsty');

SELECT reltablespace FROM pg_class where oid = 'alter_regaocsty'::regclass::oid;

SELECT count() FROM yezzey_offload_relation_status('alter_regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('alter_regaocsty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('alter_regaocsty');

SELECT count(1) FROM alter_regaocsty;
INSERT INTO alter_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM alter_regaocsty;

SELECT count() FROM yezzey_offload_relation_status('alter_regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('alter_regaocsty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('alter_regaocsty');

DELETE FROM alter_regaocsty;
INSERT INTO alter_regaocsty SELECT * FROM generate_series(1, 100000);
INSERT INTO alter_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM alter_regaocsty;

ALTER TABLE alter_regaoty SET DISTRIBUTED RANDOMLY;
SELECT count(1) FROM alter_regaocsty;

SELECT count() FROM yezzey_offload_relation_status('alter_regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('alter_regaocsty');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('alter_regaocsty');

DROP TABLE alter_regaocsty;

\! echo AOCS simple test OK

DROP EXTENSION yezzey;
CHECKPOINT;
