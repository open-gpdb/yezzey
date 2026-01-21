CREATE EXTENSION yezzey;
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

DROP TABLE regaostat;

DROP EXTENSION yezzey;
CHECKPOINT;

