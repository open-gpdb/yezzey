

CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
set allow_system_table_mods TO on;

update gp_distribution_policy set numsegments = 2 where localoid = 'regaoty'::regclass::oid;
SELECT * FROM yezzey_define_offload_policy('regaoty');

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM regaoty;

CREATE TEMP TABLE yezzey_ao_relfilenode_before_expand AS 
SELECT relfilenode FROM yezzey_dump_virtual_index('regaoty') DISTRIBUTED RANDOMLY;

ALTER TABLE regaoty EXPAND TABLE;

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM regaoty;

SELECT relfilenode FROM yezzey_dump_virtual_index('regaoty') INTERSECT TABLE yezzey_ao_relfilenode_before_expand;

DROP TABLE regaoty;

-- AOCS

CREATE TABLE regaocsty(i INT) WITH (appendonly=true);
set allow_system_table_mods TO on;

update gp_distribution_policy set numsegments = 2 where localoid = 'regaocsty'::regclass::oid;

SELECT * FROM yezzey_define_offload_policy('regaocsty');

INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM regaocsty;

CREATE TEMP TABLE yezzey_aocs_relfilenode_before_expand AS 
SELECT relfilenode FROM yezzey_dump_virtual_index('regaocsty') DISTRIBUTED RANDOMLY;

ALTER TABLE regaocsty EXPAND TABLE;

INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM regaocsty;

SELECT relfilenode FROM yezzey_dump_virtual_index('regaocsty') INTERSECT TABLE yezzey_aocs_relfilenode_before_expand;

DROP TABLE regaocsty;

DROP EXTENSION yezzey;
CHECKPOINT;
