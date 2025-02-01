CREATE EXTENSION yezzey;
SET client_min_messages TO WARNING;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaoty');

SELECT reltablespace FROM pg_class where oid = 'regaoty'::regclass::oid;

SELECT count() FROM yezzey_offload_relation_status('regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('regaoty');
--SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT count(1) FROM regaoty;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaoty;

SELECT count() FROM yezzey_offload_relation_status('regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('regaoty');
--SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaoty');

DELETE FROM regaoty;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaoty;

TRUNCATE regaoty;
SELECT count(1) FROM regaoty;

SELECT count() FROM yezzey_offload_relation_status('regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('regaoty');
--SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaoty');

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaoty;

DROP TABLE regaoty;

\! echo AO simple test OK

-- AOCS

CREATE TABLE regaocsty(i INT) WITH (appendonly=true, orientation=column);
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaocsty');

SELECT reltablespace FROM pg_class where oid = 'regaocsty'::regclass::oid;

SELECT count() FROM yezzey_offload_relation_status('regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
--SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaocsty');

SELECT count(1) FROM regaocsty;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaocsty;

SELECT count() FROM yezzey_offload_relation_status('regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
--SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaocsty');

DELETE FROM regaocsty;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaocsty;

TRUNCATE regaocsty;
SELECT count(1) FROM regaocsty;

SELECT count() FROM yezzey_offload_relation_status('regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
--SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaocsty');

INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaocsty;

DROP TABLE regaocsty;
\! echo AOCS simple test OK

DROP EXTENSION yezzey;
CHECKPOINT;
