/* Here will be yezzey_offload_status* modern functions */

reindex index yezzey.offload_metadata_indx;
reindex index yezzey.yezzey_virtual_index_idx;



DROP FUNCTION yezzey_offload_relation_status_internal(OID);

CREATE FUNCTION yezzey_offload_relation_status_internal(reloid OID) 
RETURNS TABLE (reloid OID, segindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_offload_relation_status_modern'
VOLATILE
LANGUAGE C STRICT;


DROP FUNCTION yezzey_offload_relation_status_per_filesegment(OID);

-- more detailed debug about relations file segments
CREATE FUNCTION yezzey_offload_relation_status_per_filesegment(reloid OID) 
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_offload_relation_status_per_filesegment_modern'
VOLATILE
LANGUAGE C STRICT;

DROP FUNCTION yezzey_offload_relation_status(
    TEXT, TEXT
);

CREATE FUNCTION yezzey_offload_relation_status(
    i_nspname TEXT,
    i_relname TEXT
) 
RETURNS TABLE (
    offload_reloid OID,
    segindex INTEGER,
    local_bytes BIGINT,
    external_bytes BIGINT,
    external_bloat_bytes BIGINT)
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
        relname = i_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_nspname);

    RETURN QUERY SELECT 
        y.reloid, y.segindex, y.local_bytes, y.external_bytes, y.external_bloat_bytes
    FROM yezzey_offload_relation_status_internal(
        v_reloid
    ) y;
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


DROP FUNCTION yezzey_offload_relation_status(
    TEXT
);

CREATE FUNCTION yezzey_offload_relation_status(i_relname TEXT) 
RETURNS TABLE (
    offload_reloid OID,
    segindex INTEGER,
    local_bytes BIGINT,
    external_bytes BIGINT,
    external_bloat_bytes BIGINT)
AS $$
BEGIN
    RETURN QUERY SELECT 
        *
    FROM yezzey_offload_relation_status(
        'public',
        i_relname
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;

DROP FUNCTION yezzey_offload_relation_status_per_filesegment(TEXT, TEXT);

CREATE FUNCTION yezzey_offload_relation_status_per_filesegment(
    i_nspname TEXT,
    i_relname TEXT
    ) 
RETURNS TABLE (
    offload_reloid OID,
    segindex INTEGER,
    segfileindex INTEGER,
    local_bytes BIGINT,
    external_bytes BIGINT,
    external_bloat_bytes BIGINT)
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
        relname = i_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_nspname);

    RETURN QUERY SELECT 
        y.reloid, y.segindex, y.segfileindex, y.local_bytes, y.external_bytes, y.external_bloat_bytes
    FROM yezzey_offload_relation_status_per_filesegment(
        v_reloid
    ) y;
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;

DROP FUNCTION yezzey_offload_relation_status_per_filesegment(TEXT);

CREATE FUNCTION yezzey_offload_relation_status_per_filesegment(i_relname TEXT) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
BEGIN
 
    RETURN QUERY SELECT 
        *
    FROM yezzey_offload_relation_status_per_filesegment(
        'public',
        i_relname
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;
