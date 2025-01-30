
CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaoty');

SELECT count(1) FROM yezzey_dump_virtual_index('regaoty');

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

SELECT count(1) FROM regaoty;

SELECT * FROM regaoty ORDER BY i LIMIT 5 OFFSET 7823;

SELECT segindex,external_bytes FROM yezzey_offload_relation_status('regaoty');
SELECT segindex,segfileindex,external_bytes FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT segindex,segfileindex,external_bytes FROM yezzey_relation_describe_external_storage_structure('regaoty');

