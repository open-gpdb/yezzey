

CREATE OR REPLACE FUNCTION yezzey_create_aux_virtual_index(
) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


SELECT yezzey_create_aux_virtual_index();
