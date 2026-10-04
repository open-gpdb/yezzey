DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_offload_relation('regaoty');

SELECT * FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'regaoty'::regclass::oid;

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

SELECT count(1) FROM regaoty;

SELECT * FROM regaoty LIMIT 5 OFFSET 7823;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT * FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'regaoty'::regclass::oid;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

INSERT INTO regaoty SELECT * FROM regaoty;
SELECT count(1) FROM regaoty;

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT * FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'regaoty'::regclass::oid;

\c postgres
DROP DATABASE testreg;
