
CREATE EXTENSION yezzey;

SET client_min_messages TO WARNING;

-- AO

CREATE TABLE regaotya(i INT, j INT) WITH (appendonly=true);
INSERT INTO regaotya SELECT i,i + 1 FROM generate_series(1, 100000) i;
SELECT * FROM yezzey_define_offload_policy('regaotya');
SELECT reltablespace FROM pg_class where oid = 'regaotya'::regclass::oid;

SELECT count(1) from regaotya;

-- drop columnn
ALTER TABLE regaotya DROP COLUMN j;
SELECT count(1) from regaotya;

SELECT count() FROM yezzey_offload_relation_status('regaotya');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('regaotya');
--SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaotya');

DROP TABLE regaotya;

-- AOCS 
CREATE TABLE regaocstya(i INT, j INT) WITH (appendonly=true, orientation=column);
INSERT INTO regaocstya SELECT i,i + 1 FROM generate_series(1, 100000) i;
SELECT * FROM yezzey_define_offload_policy('regaocstya');
SELECT reltablespace FROM pg_class where oid = 'regaocstya'::regclass::oid;

SELECT count(1) from regaocstya;

-- drop columnn
ALTER TABLE regaocstya DROP COLUMN j;
SELECT count(1) from regaocstya;

SELECT count() FROM yezzey_offload_relation_status('regaocstya');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('regaocstya');
--SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaocstya');

DROP TABLE regaocstya;

DROP EXTENSION yezzey;
CHECKPOINT;
