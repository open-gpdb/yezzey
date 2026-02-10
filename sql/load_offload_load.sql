
CREATE EXTENSION yezzey VERSION '1.0';

SET client_min_messages TO WARNING;

-- AO

CREATE TABLE regaotylol(i INT) WITH (appendonly=true);
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('regaotylol');
SELECT reltablespace FROM pg_class where oid = 'regaotylol'::regclass::oid;

SELECT count(1) FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotylol;

DELETE FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
VACUUM regaotylol;
SELECT count(1) FROM regaotylol;

SELECT reltablespace FROM pg_class where oid = 'regaotylol'::regclass::oid;

SELECT yezzey_load_relation('regaotylol');
SELECT reltablespace FROM pg_class where oid = 'regaotylol'::regclass::oid;

SELECT count(1) FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotylol;


SELECT * FROM yezzey_define_offload_policy('regaotylol');
SELECT reltablespace FROM pg_class where oid = 'regaotylol'::regclass::oid;

SELECT count(1) FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotylol;

DELETE FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
VACUUM regaotylol;
SELECT count(1) FROM regaotylol;

DROP TABLE regaotylol;

DROP EXTENSION yezzey;
CHECKPOINT;
