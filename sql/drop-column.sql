
CREATE EXTENSION yezzey;

SET client_min_messages TO WARNING;

-- AO

CREATE TABLE drop_column_regaotya(i INT, j INT) WITH (appendonly=true);
INSERT INTO drop_column_regaotya SELECT i,i + 1 FROM generate_series(1, 100000) i;
SELECT * FROM yezzey_define_offload_policy('drop_column_regaotya');
SELECT reltablespace FROM pg_class where oid = 'drop_column_regaotya'::regclass::oid;

SELECT count(1) from drop_column_regaotya;

-- drop columnn
ALTER TABLE drop_column_regaotya DROP COLUMN j;
SELECT count(1) from drop_column_regaotya;

SELECT count() FROM yezzey_offload_relation_status('drop_column_regaotya');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('drop_column_regaotya');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('drop_column_regaotya');

DROP TABLE drop_column_regaotya;

-- AOCS 
CREATE TABLE drop_column_regaocstya(i INT, j INT) WITH (appendonly=true, orientation=column);
INSERT INTO drop_column_regaocstya SELECT i,i + 1 FROM generate_series(1, 100000) i;
SELECT * FROM yezzey_define_offload_policy('drop_column_regaocstya');
SELECT reltablespace FROM pg_class where oid = 'drop_column_regaocstya'::regclass::oid;

SELECT count(1) from drop_column_regaocstya;

-- drop columnn
ALTER TABLE drop_column_regaocstya DROP COLUMN j;
SELECT count(1) from drop_column_regaocstya;

SELECT count() FROM yezzey_offload_relation_status('drop_column_regaocstya');
SELECT count() FROM yezzey_offload_relation_status_per_filesegment('drop_column_regaocstya');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('drop_column_regaocstya');

DROP TABLE drop_column_regaocstya;

DROP EXTENSION yezzey;
CHECKPOINT;
