
CREATE EXTENSION yezzey;

SET client_min_messages TO WARNING;
-- AO

CREATE TABLE vac_relation_regaotyvi(i INT) WITH (appendonly=true);
INSERT INTO vac_relation_regaotyvi SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('vac_relation_regaotyvi');

SELECT reltablespace FROM pg_class where oid = 'vac_relation_regaotyvi'::regclass::oid;

SELECT count(1) FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'vac_relation_regaotyvi'::regclass::oid;

INSERT INTO vac_relation_regaotyvi SELECT * FROM generate_series(1, 100000);

SELECT count(1) FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'vac_relation_regaotyvi'::regclass::oid;

DELETE FROM vac_relation_regaotyvi;

INSERT INTO vac_relation_regaotyvi SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'vac_relation_regaotyvi'::regclass::oid;

-- list external storage
SELECT count() FROM yezzey_relation_describe_external_storage_structure('vac_relation_regaotyvi');

VACUUM vac_relation_regaotyvi;

SELECT count(1) FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'vac_relation_regaotyvi'::regclass::oid;
SELECT count() FROM yezzey_relation_describe_external_storage_structure('vac_relation_regaotyvi');

SELECT count() FROM vac_relation_regaotyvi;

SELECT yezzey_vacuum_relation('vac_relation_regaotyvi', true);

SELECT count(1) FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = 'vac_relation_regaotyvi'::regclass::oid;
SELECT count() FROM yezzey_relation_describe_external_storage_structure('vac_relation_regaotyvi');

SELECT count() FROM vac_relation_regaotyvi;

SELECT segindex FROM yezzey_offload_relation_status('vac_relation_regaotyvi');
SELECT segindex,segfileindex FROM yezzey_offload_relation_status_per_filesegment('vac_relation_regaotyvi');
SELECT segindex,segfileindex FROM yezzey_relation_describe_external_storage_structure('vac_relation_regaotyvi');

SELECT count(), sum(external_bytes), sum(external_bloat_bytes) FROM yezzey_offload_relation_status('vac_relation_regaotyvi');
SELECT count(), sum(external_bytes), sum(external_bloat_bytes) FROM yezzey_offload_relation_status_per_filesegment('vac_relation_regaotyvi');
SELECT count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('vac_relation_regaotyvi');

DROP TABLE vac_relation_regaotyvi;

DROP EXTENSION yezzey;
CHECKPOINT;
