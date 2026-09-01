-- Add tablespace-level garbage vacuum wrapper

CREATE FUNCTION yezzey_vacuum_garbage_tablespace(
    tablespace OID,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;
