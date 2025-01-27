
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


CREATE OR REPLACE FUNCTION yezzey_vacuum_relation(
    relname TEXT,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
) RETURNS void
AS $$ SELECT yezzey_vacuum_relation(relname::regclass::oid, confirm, crazyDrop) $$
LANGUAGE SQL;

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


CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_to_1_8_1() RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_to_1_8_1_m() RETURNS void
AS 'MODULE_PATHNAME','yezzey_binary_upgrade_1_8_to_1_8_1'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey.yezzey_binary_upgrade_1_8_to_1_8_1_seg() 
RETURNS VOID AS 
$$
SELECT yezzey.yezzey_binary_upgrade_1_8_to_1_8_1();
$$ 
LANGUAGE SQL 
EXECUTE ON ALL SEGMENTS;

SET allow_segment_dml TO ON;

SELECT yezzey.yezzey_binary_upgrade_1_8_to_1_8_1_seg();
SELECT yezzey.yezzey_binary_upgrade_1_8_to_1_8_1_m();

RESET allow_segment_DML;

DROP FUNCTION yezzey.yezzey_binary_upgrade_1_8_to_1_8_1_seg();
DROP FUNCTION yezzey.yezzey_binary_upgrade_1_8_to_1_8_1_m();
