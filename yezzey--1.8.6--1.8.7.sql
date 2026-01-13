/* Fixes Cloudberry compatibility */

DROP FUNCTION yezzey_load_relation_seg(oid, text);
DROP FUNCTION yezzey_load_relation(OID, TEXT);
DROP FUNCTION yezzey_load_relation(TEXT, TEXT);
DROP FUNCTION yezzey_load_relation(TEXT);

CREATE FUNCTION
yezzey_load_relation_seg(reloid OID, dest_path TEXT)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE FUNCTION
yezzey_load_relation(reloid OID, dest_path TEXT)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE FUNCTION
yezzey_load_relation(load_nspname TEXT, load_relname TEXT)
RETURNS TABLE (status TEXT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
    v_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = load_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = load_nspname);

    PERFORM yezzey_load_relation_seg(
        v_reloid,
        ''-- omit dest path 
    );

    PERFORM yezzey_load_relation(
        v_reloid,
        ''-- omit dest path 
    );

    RETURN QUERY SELECT ('loaded relation '||load_relname||' to local storage')::TEXT;
END;
$$
LANGUAGE PLPGSQL;


CREATE FUNCTION
yezzey_load_relation(load_relname TEXT)
RETURNS TABLE (status TEXT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
BEGIN
    RETURN QUERY SELECT yezzey_load_relation(
        'public',
        load_relname
    );
END;
$$
LANGUAGE PLPGSQL;

DROP FUNCTION yezzey_init_metadata();

CREATE FUNCTION yezzey_init_metadata()
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

DROP FUNCTION yezzey_init_metadata_seg();

CREATE FUNCTION yezzey_init_metadata_seg()
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

DROP FUNCTION yezzey_offload_relation_to_external_path(OID, BOOLEAN, TEXT);

DROP FUNCTION yezzey_offload_relation(OID, BOOLEAN);

CREATE FUNCTION 
yezzey_offload_relation(reloid OID, remove_locally BOOLEAN)
RETURNS TABLE (status OID)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

DROP FUNCTION yezzey_set_relation_expirity_seg(OID, INT, TIMESTAMP);

CREATE FUNCTION yezzey_set_relation_expirity_seg(
    i_reloid OID,
    i_relpolicy INT,
    i_relexp TIMESTAMP
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;

DROP FUNCTION yezzey_relation_expirity_seg(OID, INT, TIMESTAMP);

CREATE FUNCTION yezzey_relation_expirity_seg(
    i_reloid OID,
    i_relpolicy INT,
    i_relexp TIMESTAMP
)
RETURNS TABLE (status BOOLEAN)
AS $$ 
SELECT yezzey_set_relation_expirity_seg(i_reloid, i_relpolicy, i_relexp);
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE SQL;