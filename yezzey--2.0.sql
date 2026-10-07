\echo Use "CREATE EXTENSION yezzey" to load this file. \quit


-- since GP uses segment-file discovery technique
-- in can fail to remove some AO/AOCS relation files locally
-- in cases when table write happened after folloading
-- see ao_foreach_extent_file

CREATE SCHEMA yezzey;

-- yezzey_expire_hint is used by yproxy
CREATE TABLE yezzey.yezzey_expire_hint(
    lsn PG_LSN,
    x_path TEXT UNIQUE
)
DISTRIBUTED BY (x_path);


CREATE FUNCTION yezzey.init_metadata()
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_init_metadata'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE FUNCTION yezzey.init_metadata_seg()
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_init_metadata'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

-- manually/automatically relocated relations
-- this creates virtual index relation, etc
SELECT yezzey.init_metadata();
SELECT yezzey.init_metadata_seg();

CREATE FUNCTION yezzey.delete_chunk(
    external_storage_path TEXT
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_delete_chunk'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

GRANT USAGE ON SCHEMA yezzey to public;

GRANT SELECT ON yezzey.offload_metadata TO PUBLIC;


-- external bytes always commited

CREATE FUNCTION yezzey.offload_relation_status(relation regclass)
RETURNS TABLE (reloid OID, segindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_offload_relation_status_modern'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


-- more detailed debug about relations file segments
CREATE FUNCTION yezzey.offload_relation_status_per_filesegment(relation regclass)
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_offload_relation_status_per_filesegment_modern'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


-- even more detailed debug about relations file segments
CREATE FUNCTION yezzey.relation_describe_external_storage_structure(relation regclass)
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, external_storage_filepath TEXT, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_relation_describe_external_storage_structure_internal'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE TABLE yezzey.offload_tablespace_map(
    reloid                 OID PRIMARY KEY,
    origin_tablespace_name NAME
) DISTRIBUTED REPLICATED;


CREATE FUNCTION yezzey.define_relation_offload_policy_internal(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_define_relation_offload_policy_internal'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE FUNCTION yezzey.define_relation_offload_policy_internal_seg(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_define_relation_offload_policy_internal_seg'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey.define_relation_offload_policy_internal_prepare_master(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME','yezzey_define_relation_offload_policy_internal_prepare'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;


CREATE FUNCTION yezzey.define_relation_offload_policy_internal_prepare(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_define_relation_offload_policy_internal_prepare'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey.offload_relation(relation regclass)
RETURNS VOID
AS $$
BEGIN   
    IF EXISTS (
        SELECT
        FROM yezzey.offload_metadata
        WHERE reloid = relation::oid AND relpolicy = 1
    ) THEN
        RAISE WARNING 'The relation has already been offloaded';
        RETURN;
    END IF;

    PERFORM yezzey.define_relation_offload_policy_internal_prepare(
        relation
    );

    PERFORM yezzey.define_relation_offload_policy_internal_prepare_master(
        relation
    );

    -- non-partitioned relation
    PERFORM yezzey.define_relation_offload_policy_internal_seg(
        relation
    );
    PERFORM yezzey.define_relation_offload_policy_internal(
        relation
    );
END;
$$
LANGUAGE PLPGSQL;


CREATE FUNCTION yezzey.delete_obsolete(
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_delete_obsolete'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey.collect_obsolete()
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_collect_obsolete'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey.vacuum(
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_vacuum'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey.vacuum_relation(
    relation regclass,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_vacuum_relation'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


-- Add tablespace-level garbage vacuum wrapper

CREATE FUNCTION yezzey.vacuum_tablespace(
    tablespace OID,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_vacuum_tablespace'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey.load_relation_internal(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_load_relation'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE FUNCTION yezzey.load_relation_internal_seg(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_load_relation'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION yezzey.load_relation(relation regclass)
RETURNS VOID
AS $$
BEGIN
    PERFORM yezzey.load_relation_internal_seg(relation);
    PERFORM yezzey.load_relation_internal(relation);
END;
$$
LANGUAGE PLPGSQL;
