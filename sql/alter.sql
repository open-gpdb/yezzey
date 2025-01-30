
CREATE EXTENSION yezzey;

SET client_min_messages TO WARNING;

-- AO

CREATE TABLE regaotya(i INT, j INT) WITH (appendonly=true);
INSERT INTO regaotya SELECT i,i + 1 FROM generate_series(1, 100000) i;
SELECT * FROM yezzey_define_offload_policy('regaotya');
SELECT reltablespace FROM pg_class where oid = 'regaotya'::regclass::oid;

SELECT count(1) from regaotya;

ALTER TABLE regaotya DROP COLUMN j;
SELECT count(1) from regaotya;

DROP TABLE regaotya;

DROP EXTENSION yezzey;
CHECKPOINT;
