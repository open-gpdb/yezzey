DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_offload_relation('regaoty');

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5 OFFSET 7823;

SELECT * FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'regaoty'::regclass::oid;

ALTER TABLE regaoty ADD COLUMN j INT;

SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5 OFFSET 7823;

INSERT INTO regaoty SELECT *, 1 FROM generate_series(1, 100000);

SELECT * FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'regaoty'::regclass::oid;
SELECT count(1) FROM regaoty;

\c postgres

DROP DATABASE testreg;
