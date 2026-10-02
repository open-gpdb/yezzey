-- Simplify relation functions with schema/name text arguments.
-- Resolve relation names internally through regclass.

CREATE FUNCTION yezzey_load_relation(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE FUNCTION yezzey_load_relation_seg(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

-- Simplify loading by resolving the relation OID through regclass.

CREATE OR REPLACE FUNCTION yezzey_load_relation(
    load_nspname TEXT,
    load_relname TEXT
)
RETURNS TABLE (status TEXT)
AS $$
DECLARE
    v_reloid OID;
BEGIN
    v_reloid := format('%I.%I', load_nspname, load_relname)::regclass::oid;

    PERFORM yezzey_load_relation_seg(v_reloid);
    PERFORM yezzey_load_relation(v_reloid);

    RETURN QUERY
    SELECT ('loaded relation ' || load_relname || ' to local storage')::TEXT;
END;
$$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION yezzey_offload_relation_status(
    i_nspname TEXT,
    i_relname TEXT
)
RETURNS TABLE (
    offload_reloid OID,
    segindex INTEGER,
    local_bytes BIGINT,
    external_bytes BIGINT,
    external_bloat_bytes BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        y.reloid,
        y.segindex,
        y.local_bytes,
        y.external_bytes,
        y.external_bloat_bytes
    FROM yezzey_offload_relation_status_internal(
        format('%I.%I', i_nspname, i_relname)::regclass::oid
    ) AS y;
END;
$$
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION yezzey_offload_relation_status_per_filesegment(
    i_nspname TEXT,
    i_relname TEXT
)
RETURNS TABLE (
    offload_reloid OID,
    segindex INTEGER,
    segfileindex INTEGER,
    local_bytes BIGINT,
    external_bytes BIGINT,
    external_bloat_bytes BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        y.reloid,
        y.segindex,
        y.segfileindex,
        y.local_bytes,
        y.external_bytes,
        y.external_bloat_bytes
    FROM yezzey_offload_relation_status_per_filesegment(
        format('%I.%I', i_nspname, i_relname)::regclass::oid
    ) AS y;
END;
$$
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION yezzey_relation_describe_external_storage_structure(
    i_nspname TEXT,
    i_relname TEXT
)
RETURNS TABLE (
    offload_reloid OID,
    segindex INTEGER,
    segfileindex INTEGER,
    external_storage_filepath TEXT,
    local_bytes BIGINT,
    local_commited_bytes BIGINT,
    external_bytes BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT *
    FROM yezzey_relation_describe_external_storage_structure_internal(
        format('%I.%I', i_nspname, i_relname)::regclass::oid
    );
END;
$$
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION yezzey_vacuum_garbage_relation(
    i_offload_nspname TEXT,
    i_offload_relname TEXT,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS $$
BEGIN
    RETURN QUERY
    SELECT *
    FROM yezzey_vacuum_relation(
        format('%I.%I', i_offload_nspname, i_offload_relname)::regclass::oid,
        confirm,
        crazyDrop
    );
END;
$$
LANGUAGE PLPGSQL;
