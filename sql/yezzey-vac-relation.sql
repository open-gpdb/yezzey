
CREATE EXTENSION yezzey;

ALTER EXTENSION yezzey UPDATE TO '1.8.3';

SET client_min_messages TO WARNING;
-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('regaoty');

SELECT reltablespace FROM pg_class where oid = 'regaoty'::regclass::oid;

SELECT count(1) FROM yezzey_dump_virtual_index('regaoty');

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

SELECT count(1) FROM yezzey_dump_virtual_index('regaoty');

DELETE FROM regaoty;

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM yezzey_dump_virtual_index('regaoty');

-- list external storage
SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaoty');

VACUUM regaoty;

SELECT count(1) FROM yezzey_dump_virtual_index('regaoty');
SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT yezzey_vacuum_relation('regaoty', true);

SELECT count(1) FROM yezzey_dump_virtual_index('regaoty');
SELECT count() FROM yezzey_relation_describe_external_storage_structure('regaoty');

-- TODO: check
--SELECT segindex,external_bytes FROM yezzey_offload_relation_status('regaoty');
--SELECT segindex,segfileindex,external_bytes FROM yezzey_offload_relation_status_per_filesegment('regaoty');

DROP TABLE regaoty;

DROP EXTENSION yezzey;
CHECKPOINT;
