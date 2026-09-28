-- Add a master-side loader overload without the destination-path parameter.

CREATE FUNCTION yezzey_load_relation(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

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
