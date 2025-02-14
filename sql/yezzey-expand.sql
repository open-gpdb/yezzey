CREATE EXTENSION yezzey;
SET client_min_messages TO WARNING;

-- AO

CREATE TABLE expand_regaoty(i INT) WITH (appendonly=true);
set allow_system_table_mods TO on;

update gp_distribution_policy set numsegments = 2 where localoid = 'expand_regaoty'::regclass::oid;

SELECT * FROM yezzey_define_offload_policy('expand_regaoty');
SELECT reltablespace FROM pg_class where oid = 'expand_regaoty'::regclass::oid;

INSERT INTO expand_regaoty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM expand_regaoty;

CREATE TEMP TABLE yezzey_ao_relfilenode_before_expand AS 
SELECT relfilenode FROM yezzey_dump_virtual_index('expand_regaoty') DISTRIBUTED RANDOMLY;

ALTER TABLE expand_regaoty EXPAND TABLE;
SELECT reltablespace FROM pg_class where oid = 'expand_regaoty'::regclass::oid;

INSERT INTO expand_regaoty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM expand_regaoty;

SELECT relfilenode FROM yezzey_dump_virtual_index('expand_regaoty') INTERSECT TABLE yezzey_ao_relfilenode_before_expand;

DROP TABLE expand_expand_regaoty;

-- AOCS

CREATE TABLE expand_regaocsty(i INT) WITH (appendonly=true);
set allow_system_table_mods TO on;

update gp_distribution_policy set numsegments = 2 where localoid = 'expand_regaocsty'::regclass::oid;

SELECT * FROM yezzey_define_offload_policy('expand_regaocsty');
SELECT reltablespace FROM pg_class where oid = 'expand_regaocsty'::regclass::oid;

INSERT INTO expand_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM expand_regaocsty;

CREATE TEMP TABLE yezzey_aocs_relfilenode_before_expand AS 
SELECT relfilenode FROM yezzey_dump_virtual_index('expand_regaocsty') DISTRIBUTED RANDOMLY;

ALTER TABLE expand_regaocsty EXPAND TABLE;
SELECT reltablespace FROM pg_class where oid = 'expand_regaocsty'::regclass::oid;

INSERT INTO expand_regaocsty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM expand_regaocsty;

SELECT relfilenode FROM yezzey_dump_virtual_index('expand_regaocsty') INTERSECT TABLE yezzey_aocs_relfilenode_before_expand;

DROP TABLE expand_regaocsty;

DROP EXTENSION yezzey;
CHECKPOINT;
