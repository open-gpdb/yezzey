
CREATE EXTENSION yezzey;

SET client_min_messages TO WARNING;
-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('regaoty');

SELECT reltablespace FROM pg_class where oid = 'regaoty'::regclass::oid;

SELECT count(1) FROM yezzey_dump_virtual_index('regaoty');

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaoty;

UPDATE regaoty SET i = i + 1;
SELECT count(1) FROM regaoty;

DELETE FROM regaoty WHERE i < 50501;
SELECT count(1) FROM regaoty;

SELECT * FROM regaoty ORDER BY i LIMIT 5 OFFSET 7823;

SELECT segindex FROM yezzey_offload_relation_status('regaoty');
SELECT segindex,segfileindex FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT segindex,segfileindex FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('regaoty');

DROP TABLE regaoty;

DROP EXTENSION yezzey;
CHECKPOINT;
