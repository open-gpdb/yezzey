CREATE EXTENSION yezzey;
ALTER EXTENSION yezzey UPDATE TO '1.8.7';

SET client_min_messages TO WARNING;

-- AO

CREATE TABLE regaotylol187(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
INSERT INTO regaotylol187 SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('regaotylol187');

SELECT reltablespace FROM pg_class where oid = 'regaotylol187'::regclass::oid;
SELECT yezzey_load_relation('regaotylol187');
SELECT reltablespace FROM pg_class where oid = 'regaotylol187'::regclass::oid;

SELECT * FROM yezzey_define_offload_policy('regaotylol187');
SELECT reltablespace FROM pg_class where oid = 'regaotylol187'::regclass::oid;

SELECT count(1) FROM regaotylol187;
INSERT INTO regaotylol187 SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotylol187;

DROP TABLE regaotylol187;

DROP EXTENSION yezzey;
CHECKPOINT;
