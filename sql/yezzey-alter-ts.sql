CREATE EXTENSION yezzey;

\! rm -fr /tmp/test_spc_tab1 && mkdir -p /tmp/test_spc_tab1

CREATE TABLESPACE tab1 LOCATION '/tmp/test_spc_tab1';

CREATE TABLE regao_y_ats(i int) WITH (appendonly=true) TABLESPACE tab1 DISTRIBUTED BY (i);

SELECT yezzey_define_offload_policy('regao_y_ats');

--should fail
ALTER TABLE regao_y_ats SET TABLESPACE pg_default;

DROP TABLE regao_y_ats;

DROP EXTENSION yezzey;
DROP TABLESPACE tab1;
CHECKPOINT;
