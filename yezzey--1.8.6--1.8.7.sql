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
