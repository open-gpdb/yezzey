CREATE EXTENSION yezzey;

create table regao_tt_alr_off (i int) with (appendonly=true) TABLESPACE "yezzey(cloud-storage)" DISTRIBUTED BY (i);

INSERT INTO regao_tt_alr_off SELECT generate_series(1,10010);
SELECT count(1) FROM regao_tt_alr_off;

SELECT segindex, local_bytes, external_bytes FROM yezzey_offload_relation_status('regao_tt_alr_off');

ALTER TABLE regao_tt_alr_off ADD COLUMN z INT;
INSERT INTO regao_tt_alr_off SELECT i, i FROM generate_series(1,10010) i;
SELECT count(1) FROM regao_tt_alr_off;

SELECT segindex, local_bytes, external_bytes FROM yezzey_offload_relation_status('regao_tt_alr_off');

SELECT yezzey_load_relation('regao_tt_alr_off');

SELECT count(1) FROM regao_tt_alr_off;

SELECT segindex, local_bytes, external_bytes FROM yezzey_offload_relation_status('regao_tt_alr_off');

DROP TABLE regao_tt_alr_off;

DROP EXTENSION yezzey;
CHECKPOINT;