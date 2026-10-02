-- Add loader overloads without the destination-path parameter.

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

-- Simplify by resolving the relation OID via regclass.

CREATE OR REPLACE FUNCTION yezzey_load_relation(load_nspname TEXT, load_relname TEXT)
RETURNS TABLE (status TEXT)
AS $$
DECLARE
    v_reloid OID;
BEGIN
    v_reloid := format('%I.%I', load_nspname, load_relname)::regclass::oid;

    PERFORM yezzey_load_relation_seg(v_reloid);
    PERFORM yezzey_load_relation(v_reloid);

    RETURN QUERY SELECT ('loaded relation ' || load_relname || ' to local storage')::TEXT;
END;
$$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION yezzey_define_offload_policy(
    i_offload_nspname TEXT,
    i_offload_relname TEXT,
    i_policy offload_policy DEFAULT 'remote_always'
)
RETURNS TABLE (status TEXT)
AS $$
DECLARE
    v_tmprow OID;
    v_reloid OID;
BEGIN
    v_reloid := format('%I.%I', i_offload_nspname, i_offload_relname)::regclass::oid;

    SELECT
        reloid
    FROM
        yezzey.offload_metadata
    INTO v_tmprow
    WHERE
        reloid = v_reloid AND relpolicy = 1;

    IF FOUND THEN
        RETURN QUERY SELECT 'relation ' || i_offload_relname || ' already offloaded';
    END IF;

    PERFORM yezzey_define_relation_offload_policy_internal_prepare(
        v_reloid
    );

    PERFORM yezzey_define_relation_offload_policy_internal_prepare_master(
        v_reloid
    );

    PERFORM yezzey_define_relation_offload_policy_internal_seg(
        v_reloid
    );
    PERFORM yezzey_define_relation_offload_policy_internal(
        v_reloid
    );

    RETURN QUERY SELECT ('offloaded relation ' || i_offload_nspname ||'.'|| i_offload_relname || ' to external storage' )::TEXT;
END;
$$
LANGUAGE PLPGSQL;
