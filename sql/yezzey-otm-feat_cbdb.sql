
CREATE EXTENSION yezzey;

SET client_min_messages TO WARNING;
-- AO

\! rm -fr /tmp/test_spc_tab1 && mkdir -p /tmp/test_spc_tab1

CREATE TABLESPACE tab1 LOCATION '/tmp/test_spc_tab1';

\! rm -fr /tmp/test_spc_tab2 && mkdir -p /tmp/test_spc_tab2

CREATE TABLESPACE tab2 LOCATION '/tmp/test_spc_tab2';

CREATE TABLE yezzey_otm_regaoty_1(i INT, s TEXT) WITH (appendonly=true) TABLESPACE tab1;
INSERT INTO yezzey_otm_regaoty_1 SELECT i, 'sdsd' FROM generate_series(1, 100000) i;

ALTER TABLE yezzey_otm_regaoty_1 ALTER COLUMN s SET STORAGE EXTERNAL;

INSERT INTO yezzey_otm_regaoty_1 VALUES (1111, repeat('a', 2323323));

CREATE TABLE yezzey_otm_regaoty_2(i INT, s TEXT) WITH (appendonly=true) TABLESPACE tab2;
INSERT INTO yezzey_otm_regaoty_2 SELECT i, 'dssd' FROM generate_series(1, 100000) i;

ALTER TABLE yezzey_otm_regaoty_2 ALTER COLUMN s SET STORAGE EXTERNAL;
INSERT INTO yezzey_otm_regaoty_2 VALUES (1111, repeat('a', 2323323));

SELECT * FROM yezzey_define_offload_policy('yezzey_otm_regaoty_1');

SELECT reltablespace FROM pg_class where oid = 'yezzey_otm_regaoty_1'::regclass::oid;

SELECT count(1) FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_1');

INSERT INTO yezzey_otm_regaoty_1 SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM yezzey_otm_regaoty_1;

UPDATE yezzey_otm_regaoty_1 SET i = i + 1;
SELECT count(1) FROM yezzey_otm_regaoty_1;

DELETE FROM yezzey_otm_regaoty_1 WHERE i < 50501;
SELECT count(1) FROM yezzey_otm_regaoty_1;

SELECT i FROM yezzey_otm_regaoty_1 ORDER BY i LIMIT 5 OFFSET 7823;

SELECT segindex,external_bytes FROM yezzey_offload_relation_status('yezzey_otm_regaoty_1') order by segindex;
SELECT segindex,segfileindex,external_bytes FROM yezzey_offload_relation_status_per_filesegment('yezzey_otm_regaoty_1') order by segindex;
SELECT segindex,segfileindex,external_bytes FROM yezzey_relation_describe_external_storage_structure('yezzey_otm_regaoty_1') order by segindex;

SELECT count(), sum(external_bytes) FROM yezzey_offload_relation_status('yezzey_otm_regaoty_1');
SELECT count(), sum(external_bytes) FROM yezzey_offload_relation_status_per_filesegment('yezzey_otm_regaoty_1');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('yezzey_otm_regaoty_1');

SELECT * FROM yezzey_define_offload_policy('yezzey_otm_regaoty_2');

SELECT reltablespace FROM pg_class where oid = 'yezzey_otm_regaoty_2'::regclass::oid;

SELECT count(1) FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_2');

INSERT INTO yezzey_otm_regaoty_2 SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM yezzey_otm_regaoty_2;

UPDATE yezzey_otm_regaoty_2 SET i = i + 1;
SELECT count(1) FROM yezzey_otm_regaoty_2;

DELETE FROM yezzey_otm_regaoty_2 WHERE i < 50501;
SELECT count(1) FROM yezzey_otm_regaoty_2;

SELECT i FROM yezzey_otm_regaoty_2 ORDER BY i LIMIT 5 OFFSET 7823;

SELECT segindex,external_bytes FROM yezzey_offload_relation_status('yezzey_otm_regaoty_2') order by segindex;
SELECT segindex,segfileindex,external_bytes FROM yezzey_offload_relation_status_per_filesegment('yezzey_otm_regaoty_2') order by segindex;
SELECT segindex,segfileindex,external_bytes FROM yezzey_relation_describe_external_storage_structure('yezzey_otm_regaoty_2') order by segindex;

SELECT count(), sum(external_bytes) FROM yezzey_offload_relation_status('yezzey_otm_regaoty_2');
SELECT count(), sum(external_bytes) FROM yezzey_offload_relation_status_per_filesegment('yezzey_otm_regaoty_2');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('yezzey_otm_regaoty_2');

select reloid::regclass, origin_tablespace_name from yezzey.offload_tablespace_map order by reloid::regclass::text;

SELECT yezzey_load_relation('yezzey_otm_regaoty_1');

SELECT spcname from pg_tablespace where oid = (select reltablespace from pg_class where relname='yezzey_otm_regaoty_1');

SELECT yezzey_load_relation('yezzey_otm_regaoty_2');

SELECT spcname from pg_tablespace where oid = (select reltablespace from pg_class where relname='yezzey_otm_regaoty_2');

CREATE TABLE yezzey_otm_regaoty_1_cs(i INT, j INT, s TEXT) WITH (appendonly=true, orientation=column) TABLESPACE tab1;

INSERT INTO yezzey_otm_regaoty_1_cs SELECT i, i, md5(i::text) FROM generate_series(1, 100000) i;

SELECT * FROM yezzey_define_offload_policy('yezzey_otm_regaoty_1_cs');

SELECT yezzey_load_relation('yezzey_otm_regaoty_1_cs');

SELECT * FROM yezzey_define_offload_policy('yezzey_otm_regaoty_1_cs');

-- test simple reorgnaze & vacuum 
ALTER TABLE yezzey_otm_regaoty_1 SET WITH (reorganize=TRUE);
VACUUM FULL yezzey_otm_regaoty_1;

ALTER TABLE yezzey_otm_regaoty_2 SET WITH (reorganize=TRUE);
VACUUM FULL yezzey_otm_regaoty_2;

ALTER TABLE yezzey_otm_regaoty_1_cs SET WITH (reorganize=TRUE);
VACUUM FULL yezzey_otm_regaoty_1_cs;

DROP TABLE yezzey_otm_regaoty_1;
DROP TABLE yezzey_otm_regaoty_1_cs;
DROP TABLE yezzey_otm_regaoty_2;

DROP TABLESPACE tab1;
DROP TABLESPACE tab2;

DROP EXTENSION yezzey;
CHECKPOINT;
