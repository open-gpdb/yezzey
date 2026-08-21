CREATE EXTENSION yezzey VERSION '1.0';
ALTER EXTENSION yezzey UPDATE TO '1.8.9';

\! rm -fr /tmp/test_spc_vacuum_garbage_tab1 && mkdir -p /tmp/test_spc_vacuum_garbage_tab1
CREATE TABLESPACE vacuum_garbage_tab1 LOCATION '/tmp/test_spc_vacuum_garbage_tab1';

\! rm -fr /tmp/test_spc_vacuum_garbage_tab2 && mkdir -p /tmp/test_spc_vacuum_garbage_tab2
CREATE TABLESPACE vacuum_garbage_tab2 LOCATION '/tmp/test_spc_vacuum_garbage_tab2';

CREATE TABLE vacuum_garbage_aot(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
SELECT yezzey_define_offload_policy('vacuum_garbage_aot');

CREATE TABLE vacuum_garbage_aot_r(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
SELECT yezzey_define_offload_policy('vacuum_garbage_aot_r');

CREATE TABLE vacuum_garbage_ts1(i INT) WITH (appendonly=true) TABLESPACE vacuum_garbage_tab1 DISTRIBUTED BY (i);
SELECT yezzey_define_offload_policy('vacuum_garbage_ts1');

CREATE TABLE vacuum_garbage_ts2(i INT) WITH (appendonly=true) TABLESPACE vacuum_garbage_tab2 DISTRIBUTED BY (i);
SELECT yezzey_define_offload_policy('vacuum_garbage_ts2');

-- check how it work with ONLY given relation
INSERT INTO vacuum_garbage_aot_r VALUES(1);
DELETE FROM vacuum_garbage_aot_r;
INSERT INTO vacuum_garbage_aot_r VALUES(1);
VACUUM vacuum_garbage_aot_r;

INSERT INTO vacuum_garbage_aot VALUES(1);
DELETE FROM vacuum_garbage_aot;
INSERT INTO vacuum_garbage_aot VALUES(1);
VACUUM vacuum_garbage_aot;

INSERT INTO vacuum_garbage_ts1 VALUES(1);
DELETE FROM vacuum_garbage_ts1;
INSERT INTO vacuum_garbage_ts1 VALUES(1);
VACUUM vacuum_garbage_ts1;

INSERT INTO vacuum_garbage_ts2 VALUES(1);
DELETE FROM vacuum_garbage_ts2;
INSERT INTO vacuum_garbage_ts2 VALUES(1);
VACUUM vacuum_garbage_ts2;

-- should be three files, old and new, and after vacuum
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_r');

-- should be three files, old and new, and after vacuum
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot');

-- should be three files, old and new, and after vacuum in custom tablespaces
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts1');
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts2');

SELECT yezzey_vacuum_garbage_relation('vacuum_garbage_aot_r', true, true);

-- should single file.
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_r');

-- should not change
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot');

-- should not change custom tablespace relations before tablespace vacuum
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts1');
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts2');

-- should vacuum garbage only in requested tablespace
SELECT yezzey_vacuum_garbage_tablespace((SELECT oid FROM pg_tablespace WHERE spcname = 'vacuum_garbage_tab1'), true, true);
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts1');
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts2');

-- default tablespace relation should not change after custom tablespace vacuum
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot');

-- should be one file in each relation
SELECT yezzey_vacuum_garbage(true, true);
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot');
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_r');
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts1');
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts2');

DROP TABLE vacuum_garbage_aot;
DROP TABLE vacuum_garbage_aot_r;
DROP TABLE vacuum_garbage_ts1;
DROP TABLE vacuum_garbage_ts2;

-- should be zero
SELECT yezzey_vacuum_garbage(true, true);
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot');
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_r');
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts1');
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_ts2');

DROP TABLESPACE vacuum_garbage_tab1;
DROP TABLESPACE vacuum_garbage_tab2;

DROP EXTENSION yezzey;
CHECKPOINT;
