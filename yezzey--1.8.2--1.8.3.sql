

CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3() RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_m() RETURNS void
AS 'MODULE_PATHNAME','yezzey_binary_upgrade_1_8_2_to_1_8_3'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_seg() 
RETURNS VOID AS 
$$
SELECT yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3();
$$ 
LANGUAGE SQL 
EXECUTE ON ALL SEGMENTS;

SET allow_segment_dml TO ON;

SELECT yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_seg();
SELECT yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_m();

RESET allow_segment_DML;

DROP FUNCTION yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_seg();
DROP FUNCTION yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_m();
