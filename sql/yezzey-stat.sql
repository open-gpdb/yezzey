CREATE EXTENSION yezzey VERSION '1.0';
SET client_min_messages TO WARNING;

-- AO

CREATE TABLE regaostat(i INT) WITH (appendonly=true);
INSERT INTO regaostat SELECT * FROM generate_series(1, 100000);

SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaostat');

SELECT * FROM yezzey_define_offload_policy('regaostat');
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaostat');

SELECT reltablespace FROM pg_class where oid = 'regaostat'::regclass::oid;
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaostat');

DELETE FROM regaostat;
INSERT INTO regaostat SELECT * FROM generate_series(1, 100000);
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaostat');
VACUUM regaostat;
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaostat');


SELECT yezzey_load_relation('regaostat');
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaostat');

ALTER EXTENSION yezzey UPDATE TO '1.8.7';
SELECT sum(local_bytes), sum(external_bytes), sum(external_bloat_bytes) FROM yezzey_offload_relation_status('regaostat');


SELECT * FROM yezzey_define_offload_policy('regaostat');

SELECT sum(local_bytes), sum(external_bytes), sum(external_bloat_bytes) FROM yezzey_offload_relation_status('regaostat');

VACUUM (YEZZEY);
SELECT sum(local_bytes), sum(external_bytes), sum(external_bloat_bytes) FROM yezzey_offload_relation_status('regaostat');

DROP TABLE regaostat;


-- AOCS

CREATE TABLE regaocsstat(i INT, j INT, k INT, r INT) WITH (appendonly=true, orientation=column);
INSERT INTO regaocsstat SELECT i,i,i,i FROM generate_series(1, 100000) i;

-- TODO: fix
-- SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaocsstat');

SELECT * FROM yezzey_define_offload_policy('regaocsstat');
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaocsstat');

SELECT reltablespace FROM pg_class where oid = 'regaocsstat'::regclass::oid;
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaocsstat');

DELETE FROM regaocsstat;
INSERT INTO regaocsstat SELECT i,i,i,i FROM generate_series(1, 100000)i;
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaocsstat');
VACUUM regaocsstat;
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaocsstat');


SELECT yezzey_load_relation('regaocsstat');
SELECT sum(local_bytes), sum(external_bytes) FROM yezzey_offload_relation_status('regaocsstat');

ALTER EXTENSION yezzey UPDATE TO '1.8.7';
SELECT sum(local_bytes), sum(external_bytes), sum(external_bloat_bytes) FROM yezzey_offload_relation_status('regaocsstat');


SELECT * FROM yezzey_define_offload_policy('regaocsstat');

SELECT sum(local_bytes), sum(external_bytes), sum(external_bloat_bytes) FROM yezzey_offload_relation_status('regaocsstat');

VACUUM (YEZZEY);
SELECT sum(local_bytes), sum(external_bytes), sum(external_bloat_bytes) FROM yezzey_offload_relation_status('regaocsstat');


DROP TABLE regaocsstat;


DROP EXTENSION yezzey;
CHECKPOINT;

