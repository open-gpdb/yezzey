
CREATE OR REPLACE FUNCTION yezzey_vacuum_garbage(
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION yezzey_vacuum_relation(
    reloid OID,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey_vacuum_garbage_relation(
    i_offload_nspname TEXT,
    i_offload_relname TEXT,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
) RETURNS void
AS $$
DECLARE
    v_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = i_offload_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_offload_nspname);

    PERFORM yezzey_vacuum_relation(
        v_reloid,confirm,crazyDrop
    );
END;
$$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION
yezzey_vacuum_garbage_relation(
    i_offload_relname TEXT,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE)
RETURNS VOID
AS $$
BEGIN
    PERFORM yezzey_vacuum_garbage_relation('public', i_offload_relname, confirm, crazyDrop);
END;
$$
LANGUAGE PLPGSQL;

DROP TABLE IF EXISTS yezzey.yezzey_expire_index;

CREATE OR REPLACE FUNCTION
yezzey.fixup_stale_data() RETURNS VOID
AS
$$
    
    --DELETE FROM yezzey.yezzey_virtual_index vi WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE relfilenode = vi.filenode);

    DELETE FROM yezzey.offload_metadata op WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = op.reloid);
$$ LANGUAGE SQL
EXECUTE ON ALL SEGMENTS;

CREATE TABLE yezzey.yezzey_expire_hint
(
    x_path TEXT PRIMARY KEY,
    lsn pg_lsn
) with (appendonly=false);

SET allow_segment_DML to ON;

SELECT yezzey.fixup_stale_data();

RESET allow_segment_DML;

-- on master
DELETE FROM yezzey.offload_metadata op WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = op.reloid);

DROP FUNCTION yezzey.fixup_stale_data();

