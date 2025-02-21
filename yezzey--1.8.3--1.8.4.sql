

-- create yezzey hint index here


CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_3_to_1_8_4_m() RETURNS void
AS 'MODULE_PATHNAME','yezzey_binary_upgrade_1_8_3_to_1_8_4'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_3_to_1_8_4_seg() RETURNS void  
AS 'MODULE_PATHNAME','yezzey_binary_upgrade_1_8_3_to_1_8_4'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

SET allow_segment_dml TO ON;

SELECT yezzey.yezzey_binary_upgrade_1_8_3_to_1_8_4_seg();
SELECT yezzey.yezzey_binary_upgrade_1_8_3_to_1_8_4_m();

RESET allow_segment_DML;

DROP FUNCTION yezzey.yezzey_binary_upgrade_1_8_3_to_1_8_4_seg();
DROP FUNCTION yezzey.yezzey_binary_upgrade_1_8_3_to_1_8_4_m();
