CREATE EXTENSION yezzey;

SET client_min_messages TO WARNING;

-- AO

CREATE TABLE regaotylol187(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
INSERT INTO regaotylol187 SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('regaotylol187');

SELECT reltablespace FROM pg_class where oid = 'regaotylol187'::regclass::oid;
SELECT yezzey_load_relation('public', 'regaotylol187');
SELECT reltablespace FROM pg_class where oid = 'regaotylol187'::regclass::oid;

SELECT * FROM yezzey_define_offload_policy('regaotylol187');
SELECT reltablespace FROM pg_class where oid = 'regaotylol187'::regclass::oid;

SELECT count(1) FROM regaotylol187;
INSERT INTO regaotylol187 SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotylol187;

DROP TABLE regaotylol187;



CREATE TABLE vacuum_garbage_aot_187(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
SELECT yezzey_define_offload_policy('vacuum_garbage_aot_187');

CREATE TABLE vacuum_garbage_aot_187_r_187(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
SELECT yezzey_define_offload_policy('vacuum_garbage_aot_187_r_187');

-- check how it work with ONLY given relation
INSERT INTO vacuum_garbage_aot_187_r_187 VALUES(1);
DELETE FROM vacuum_garbage_aot_187_r_187;
INSERT INTO vacuum_garbage_aot_187_r_187 VALUES(1);
VACUUM vacuum_garbage_aot_187_r_187;

INSERT INTO vacuum_garbage_aot_187 VALUES(1);
DELETE FROM vacuum_garbage_aot_187;
INSERT INTO vacuum_garbage_aot_187 VALUES(1);
VACUUM vacuum_garbage_aot_187;

-- should be three files, old and new, and after vacuum
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_187_r_187'::regclass);

-- should be three files, old and new, and after vacuum
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_187'::regclass);

SELECT yezzey_vacuum_relation('vacuum_garbage_aot_187_r_187'::regclass, true, true) AS yezzey_vacuum_garbage_relation;

-- should single file.
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_187_r_187'::regclass);

-- should not change
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_187'::regclass);

-- should be one file in each relation
SELECT yezzey_vacuum_garbage(true, true);
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_187'::regclass);
SELECT count(1) FROM yezzey_relation_describe_external_storage_structure('vacuum_garbage_aot_187_r_187'::regclass);

CREATE TEMP TABLE vacuum_garbage_dropped_oids_187 (relname text, reloid oid) DISTRIBUTED REPLICATED;
INSERT INTO vacuum_garbage_dropped_oids_187 VALUES
    ('vacuum_garbage_aot_187', 'vacuum_garbage_aot_187'::regclass::oid),
    ('vacuum_garbage_aot_187_r_187', 'vacuum_garbage_aot_187_r_187'::regclass::oid);

DROP TABLE vacuum_garbage_aot_187;
DROP TABLE vacuum_garbage_aot_187_r_187;

-- should be zero
SELECT yezzey_vacuum_garbage(true, true);
SELECT count(1) FROM yezzey.yezzey_virtual_index WHERE relation = (SELECT reloid FROM vacuum_garbage_dropped_oids_187 WHERE relname = 'vacuum_garbage_aot_187');
SELECT count(1) FROM yezzey.yezzey_virtual_index WHERE relation = (SELECT reloid FROM vacuum_garbage_dropped_oids_187 WHERE relname = 'vacuum_garbage_aot_187_r_187');

DROP TABLE vacuum_garbage_dropped_oids_187;

DROP EXTENSION yezzey;
CHECKPOINT;
