CREATE EXTENSION yezzey VERSION '1.0';
SET client_min_messages TO WARNING;

-- AO

CREATE TABLE reorg_regaoty(i INT) WITH (appendonly=true);
INSERT INTO reorg_regaoty SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('reorg_regaoty');
SELECT reltablespace FROM pg_class where oid = 'reorg_regaoty'::regclass::oid;

SELECT count() FROM yezzey_offload_relation_status('reorg_regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('reorg_regaoty');
--SELECT * FROM yezzey_relation_describe_external_storage_structure('reorg_regaoty');

SELECT count(1) FROM reorg_regaoty;
INSERT INTO reorg_regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM reorg_regaoty;

SELECT count() FROM yezzey_offload_relation_status('reorg_regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('reorg_regaoty');
--SELECT * FROM yezzey_relation_describe_external_storage_structure('reorg_regaoty');

DELETE FROM reorg_regaoty;
INSERT INTO reorg_regaoty SELECT * FROM generate_series(1, 100000);
INSERT INTO reorg_regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM reorg_regaoty;

ALTER TABLE reorg_regaoty SET WITH (REORGANIZE=true);
SELECT reltablespace FROM pg_class where oid = 'reorg_regaoty'::regclass::oid;
SELECT count(1) FROM reorg_regaoty;

SELECT count() FROM yezzey_offload_relation_status('reorg_regaoty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('reorg_regaoty');
--SELECT * FROM yezzey_relation_describe_external_storage_structure('reorg_regaoty');

DROP TABLE reorg_regaoty;

\! echo AO simple test OK

-- AOCS

CREATE TABLE reorg_regaocsty(i INT) WITH (appendonly=true, orientation=column);
INSERT INTO reorg_regaocsty SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('reorg_regaocsty');
SELECT reltablespace FROM pg_class where oid = 'reorg_regaocsty'::regclass::oid;

SELECT count() FROM yezzey_offload_relation_status('reorg_regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('reorg_regaocsty');
--SELECT * FROM yezzey_relation_describe_external_storage_structure('reorg_regaocsty');

SELECT count(1) FROM reorg_regaocsty;
INSERT INTO reorg_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM reorg_regaocsty;

SELECT count() FROM yezzey_offload_relation_status('reorg_regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('reorg_regaocsty');
--SELECT * FROM yezzey_relation_describe_external_storage_structure('reorg_regaocsty');

DELETE FROM reorg_regaocsty;
INSERT INTO reorg_regaocsty SELECT * FROM generate_series(1, 100000);
INSERT INTO reorg_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM reorg_regaocsty;

ALTER TABLE reorg_regaocsty SET WITH (REORGANIZE=true);
SELECT count(1) FROM reorg_regaocsty;
SELECT reltablespace FROM pg_class where oid = 'reorg_regaocsty'::regclass::oid;

SELECT count() FROM yezzey_offload_relation_status('reorg_regaocsty');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('reorg_regaocsty');
--SELECT * FROM yezzey_relation_describe_external_storage_structure('reorg_regaocsty');

DROP TABLE reorg_regaocsty;

\! echo AOCS simple test OK

DROP EXTENSION yezzey;
CHECKPOINT;
