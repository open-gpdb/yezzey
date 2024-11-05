
-- New utilities & functions
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


CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_to_1_8_1() RETURNS void
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

-- will preserve NULL distrib policy
CREATE TABLE yezzey.yezzey_virtual_index_stale AS select * from yezzey.yezzey_virtual_index limit 0;
CREATE TABLE yezzey.offload_metadata_stale AS select * from yezzey.offload_metadata limit 0;

CREATE OR REPLACE FUNCTION
yezzey.fixup_stale_data() RETURNS VOID
AS
$$
    WITH stale_data AS (
        SELECT * FROM
            yezzey.yezzey_virtual_index vi 
        WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE relfilenode = vi.filenode)
    )
    INSERT INTO yezzey.yezzey_virtual_index_stale TABLE stale_data;

    DELETE FROM 
            yezzey.yezzey_virtual_index vi 
        WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE relfilenode = vi.filenode);

    WITH stale_offload_data AS (
        SELECT * FROM
            yezzey.offload_metadata op 
        WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = op.reloid)
    )
    INSERT INTO yezzey.offload_metadata_stale TABLE stale_offload_data;

    DELETE FROM 
            yezzey.offload_metadata op 
        WHERE NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = op.reloid);

$$ LANGUAGE SQL
EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_to_1_8_1_seg() 
RETURNS VOID AS 
$$
SELECT yezzey.yezzey_binary_upgrade_1_8_to_1_8_1();
$$ 
LANGUAGE SQL 
EXECUTE ON ALL SEGMENTS;

CREATE TABLE yezzey.yezzey_expire_hint
(
    x_path TEXT PRIMARY KEY,
    lsn pg_lsn
) WITH (appendonly=false);

SET allow_segment_dml TO ON;

SELECT yezzey.fixup_stale_data();
SELECT yezzey.yezzey_binary_upgrade_1_8_to_1_8_1_seg();
SELECT yezzey.yezzey_binary_upgrade_1_8_to_1_8_1();

RESET allow_segment_DML;

DROP FUNCTION yezzey.yezzey_binary_upgrade_1_8_to_1_8_1_seg();
DROP FUNCTION yezzey.fixup_stale_data();

-- CREATE INDEX yezzey_virtual_index_x_path ON yezzey.yezzey_virtual_index(x_path);


