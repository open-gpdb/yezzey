
CREATE EXTENSION yezzey VERSION '1.0';

SET client_min_messages TO WARNING;
-- AO

CREATE TABLE simple_regaoty(i INT) WITH (appendonly=true);
INSERT INTO simple_regaoty SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_define_offload_policy('simple_regaoty');

SELECT reltablespace FROM pg_class where oid = 'simple_regaoty'::regclass::oid;

SELECT count(1) FROM yezzey_dump_virtual_index('simple_regaoty');

INSERT INTO simple_regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM simple_regaoty;

UPDATE simple_regaoty SET i = i + 1;
SELECT count(1) FROM simple_regaoty;

DELETE FROM simple_regaoty WHERE i < 50501;
SELECT count(1) FROM simple_regaoty;

SELECT * FROM simple_regaoty ORDER BY i LIMIT 5 OFFSET 7823;

DO $$
DECLARE
  i INTEGER;
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM * FROM yezzey_offload_relation_status('simple_regaoty');
    PERFORM * FROM yezzey_offload_relation_status_per_filesegment('simple_regaoty');
    PERFORM * FROM yezzey_relation_describe_external_storage_structure('simple_regaoty');
  END LOOP;
END $$;

DO $$
DECLARE
  i INTEGER;
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM count(), sum(external_bytes) FROM yezzey_offload_relation_status('simple_regaoty');
    PERFORM count(), sum(external_bytes) FROM yezzey_offload_relation_status_per_filesegment('simple_regaoty');
    PERFORM count(), sum(external_bytes) FROM yezzey_relation_describe_external_storage_structure('simple_regaoty');
  END LOOP;
END $$;

DROP TABLE simple_regaoty;

DROP EXTENSION yezzey;
CHECKPOINT;
