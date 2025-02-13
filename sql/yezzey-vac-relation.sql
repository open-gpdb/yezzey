
CREATE EXTENSION yezzey;

ALTER EXTENSION yezzey UPDATE TO '1.8.3';

SET client_min_messages TO WARNING;
-- AO

CREATE TABLE regaotyvi(i INT) WITH (appendonly=true);
INSERT INTO regaotyvi SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('regaotyvi');

SELECT reltablespace FROM pg_class where oid = 'regaotyvi'::regclass::oid;

SELECT count(1) FROM yezzey_dump_virtual_index('regaotyvi');

INSERT INTO regaotyvi SELECT * FROM generate_series(1, 100000);

SELECT count(1) FROM yezzey_dump_virtual_index('regaotyvi');

DELETE FROM regaotyvi;

INSERT INTO regaotyvi SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM yezzey_dump_virtual_index('regaotyvi');

-- list external storage
SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaotyvi');

VACUUM regaotyvi;

SELECT count(1) FROM yezzey_dump_virtual_index('regaotyvi');
SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaotyvi');

SELECT count() FROM regaotyvi;

SELECT yezzey_vacuum_relation('regaotyvi', false);

SELECT count(1) FROM yezzey_dump_virtual_index('regaotyvi');
SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaotyvi');

SELECT count() FROM regaotyvi;

SELECT segindex FROM yezzey_offload_relation_status('regaotyvi');
SELECT segindex,segfileindex FROM yezzey_offload_relation_status_per_filesegment('regaotyvi');
SELECT segindex,segfileindex FROM yezzey_relation_describe_external_storage_structure('regaotyvi');

SELECT count(), sum(external_bytes) FROM yezzey_offload_relation_status('regaotyvi') WHERE offload_reloid='regaotyvi'::regclass::oid;
SELECT count(), sum(external_bytes) FROM yezzey_offload_relation_status_per_filesegment('regaotyvi') WHERE offload_reloid='regaotyvi'::regclass::oid;
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('regaotyvi') WHERE offload_reloid='regaotyvi'::regclass::oid;

DROP TABLE regaotyvi;

DROP EXTENSION yezzey;
CHECKPOINT;
