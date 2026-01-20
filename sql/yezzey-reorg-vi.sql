CREATE EXTENSION yezzey;
SET client_min_messages TO WARNING;

-- AO

CREATE TABLE vi_reorg_regaoty(i INT) WITH (appendonly=true);
INSERT INTO vi_reorg_regaoty SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('vi_reorg_regaoty');
SELECT reltablespace FROM pg_class where oid = 'vi_reorg_regaoty'::regclass::oid;

SELECT count() FROM yezzey_dump_virtual_index('vi_reorg_regaoty');

INSERT INTO vi_reorg_regaoty SELECT * FROM generate_series(1, 100000);
SELECT count() FROM vi_reorg_regaoty;

SELECT count() FROM yezzey_dump_virtual_index('vi_reorg_regaoty');

DELETE FROM vi_reorg_regaoty;
INSERT INTO vi_reorg_regaoty SELECT * FROM generate_series(1, 100000);
INSERT INTO vi_reorg_regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM vi_reorg_regaoty;

ALTER TABLE vi_reorg_regaoty SET WITH (REORGANIZE=true);
SELECT reltablespace FROM pg_class where oid = 'vi_reorg_regaoty'::regclass::oid;
SELECT count(1) FROM vi_reorg_regaoty;

SELECT count() FROM yezzey_dump_virtual_index('vi_reorg_regaoty');

DROP TABLE vi_reorg_regaoty;

DROP EXTENSION yezzey;
CHECKPOINT;
