
CREATE FUNCTION yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_m() RETURNS VOID
AS 'MODULE_PATHNAME','yezzey_binary_upgrade_1_8_2_to_1_8_3'
VOLATILE
LANGUAGE C STRICT
EXECUTE ON MASTER;


CREATE FUNCTION yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_seg() RETURNS VOID 
AS 'MODULE_PATHNAME','yezzey_binary_upgrade_1_8_2_to_1_8_3'
VOLATILE
LANGUAGE C STRICT
EXECUTE ON ALL SEGMENTS;

SELECT yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_seg();
SELECT yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_m();

DROP FUNCTION yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_seg();
DROP FUNCTION yezzey.yezzey_binary_upgrade_1_8_2_to_1_8_3_m();
