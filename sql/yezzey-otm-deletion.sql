
CREATE EXTENSION yezzey;

-- check that load result of is correct
ALTER EXTENSION yezzey UPDATE TO '1.8.6';

SET client_min_messages TO WARNING;
-- AO

\! rm -fr /tmp/test_spc_tab1 && mkdir -p /tmp/test_spc_tab1

CREATE TABLESPACE tab1 LOCATION '/tmp/test_spc_tab1';

\! rm -fr /tmp/test_spc_tab2 && mkdir -p /tmp/test_spc_tab2

CREATE TABLESPACE tab2 LOCATION '/tmp/test_spc_tab2';

-- Create tables
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
SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_1') 
WHERE x_path LIKE '/segments_005/seg2/basebackups_005/yezzey/17654%';
INSERT INTO yezzey_otm_regaoty_1 SELECT * FROM generate_series(1, 100000);
SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_1') 
WHERE x_path LIKE '/segments_005/seg2/basebackups_005/yezzey/17654%';

UPDATE yezzey_otm_regaoty_1 SET i = i + 1;

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_1') 
WHERE x_path LIKE '/segments_005/seg2/basebackups_005/yezzey/17654%';

DELETE FROM yezzey_otm_regaoty_2 WHERE i < 50501;

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_1') 
WHERE x_path LIKE '/segments_005/seg2/basebackups_005/yezzey/17654%';

VACUUM FULL yezzey_otm_regaoty_2;

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_1') 
WHERE x_path LIKE '/segments_005/seg2/basebackups_005/yezzey/17654%';

SELECT yezzey_vacuum_garbage_relation('tab2','yezzey_otm_regaoty_2');

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_1') 
WHERE x_path LIKE '/segments_005/seg2/basebackups_005/yezzey/17654%';

SELECT * FROM yezzey_relation_describe_external_storage_structure('yezzey_otm_regaoty_1');



SELECT * FROM yezzey_define_offload_policy('yezzey_otm_regaoty_2');

SELECT reltablespace FROM pg_class where oid = 'yezzey_otm_regaoty_2'::regclass::oid;

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_2');

INSERT INTO yezzey_otm_regaoty_2 SELECT * FROM generate_series(1, 100000);

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_2');

UPDATE yezzey_otm_regaoty_2 SET i = i + 1;

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_2');

DELETE FROM yezzey_otm_regaoty_2 WHERE i < 50501;

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_2');

VACUUM FULL yezzey_otm_regaoty_2;

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_2');

SELECT yezzey_vacuum_garbage_relation('tab2','yezzey_otm_regaoty_2');

SELECT x_path FROM yezzey_dump_virtual_index('yezzey_otm_regaoty_2');

SELECT * FROM yezzey_relation_describe_external_storage_structure('yezzey_otm_regaoty_2');



SELECT yezzey_load_relation('yezzey_otm_regaoty_1');

SELECT spcname from pg_tablespace where oid = (select reltablespace from pg_class where relname='yezzey_otm_regaoty_1');

SELECT yezzey_load_relation('yezzey_otm_regaoty_2');

SELECT spcname from pg_tablespace where oid = (select reltablespace from pg_class where relname='yezzey_otm_regaoty_2');

-- test simple reorgnaze & vacuum 
ALTER TABLE yezzey_otm_regaoty_1 SET WITH (reorganize=TRUE);
VACUUM FULL yezzey_otm_regaoty_1;

ALTER TABLE yezzey_otm_regaoty_2 SET WITH (reorganize=TRUE);
VACUUM FULL yezzey_otm_regaoty_2;

DROP TABLE yezzey_otm_regaoty_1;
DROP TABLE yezzey_otm_regaoty_2;

DROP TABLESPACE tab1;
DROP TABLESPACE tab2;

DROP EXTENSION yezzey;
CHECKPOINT;
