\echo Use "CREATE EXTENSION yezzey" to load this file. \quit


-- since GP uses segment-file discovery technique
-- in can fail to remove some AO/AOCS relation files locally
-- in cases when table write happened after folloading
-- see ao_foreach_extent_file
-- 

CREATE SCHEMA yezzey;

CREATE FUNCTION yezzey_init_metadata()
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE FUNCTION yezzey_init_metadata_seg()
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_init_metadata'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

-- manually/automatically relocated relations
-- this creates virtual index relation, etc
SELECT yezzey_init_metadata();
SELECT yezzey_init_metadata_seg();

CREATE FUNCTION yezzey_delete_chunk(
    external_storage_path TEXT
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

GRANT USAGE ON SCHEMA yezzey to public;

GRANT SELECT ON yezzey.offload_metadata TO PUBLIC;


-- external bytes always commited

CREATE FUNCTION yezzey_offload_relation_status(relation regclass) 
RETURNS TABLE (reloid OID, segindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_offload_relation_status_modern'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


-- more detailed debug about relations file segments
CREATE FUNCTION yezzey_offload_relation_status_per_filesegment(relation regclass) 
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_offload_relation_status_per_filesegment_modern'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


-- even more detailed debug about relations file segments
CREATE FUNCTION yezzey_relation_describe_external_storage_structure(relation regclass) 
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, external_storage_filepath TEXT, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_relation_describe_external_storage_structure_internal'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_dump_virtual_index(i_relname text) 
RETURNS 
    TABLE(
        reloid OID,
        relfilenode OID,
        blkno integer,
        offset_start bigint,
        offset_finish bigint,
        encrypted int,
        reused int,
        modcount bigint,
        lsn pg_lsn,
        x_path TEXT)
AS $$
DECLARE
    v_reloid OID;
BEGIN
    select oid from pg_class p INTO v_reloid where relname = i_relname;
    RETURN QUERY SELECT * FROM gp_dist_random('yezzey.yezzey_virtual_index') WHERE relation = v_reloid;
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE plpgsql;

CREATE TABLE yezzey.offload_tablespace_map(
    reloid                 OID PRIMARY KEY,
    origin_tablespace_name NAME
) DISTRIBUTED REPLICATED;


CREATE TABLE yezzey.yezzey_virtual_index_stale AS 
    SELECT * FROM yezzey.yezzey_virtual_index LIMIT 0;

CREATE TABLE yezzey.offload_metadata_stale AS
    SELECT * FROM yezzey.offload_metadata LIMIT 0;

CREATE FUNCTION
yezzey.yezzey_fixup_stale_metadata()
RETURNS TABLE (status BOOLEAN)
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

    SELECT false;
$$ LANGUAGE SQL
EXECUTE ON ALL SEGMENTS;


CREATE FUNCTION 
yezzey_offload_relation(reloid OID, remove_locally BOOLEAN)
RETURNS TABLE (status OID)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_define_relation_offload_policy_internal(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE FUNCTION yezzey_define_relation_offload_policy_internal_seg(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_define_relation_offload_policy_internal_prepare_master(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME','yezzey_define_relation_offload_policy_internal_prepare'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_define_relation_offload_policy_internal_prepare(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION
yezzey_define_offload_policy(relation regclass)
RETURNS TABLE (status TEXT)
AS $$
DECLARE
    v_tmprow OID;
BEGIN   
    SELECT 
        reloid
    FROM
        yezzey.offload_metadata
    INTO v_tmprow 
    WHERE 
        reloid = relation::oid AND relpolicy = 1;

    IF FOUND THEN
	    RETURN QUERY SELECT 'relation already offloaded';
    END IF;

    PERFORM yezzey_define_relation_offload_policy_internal_prepare(
        relation
    );

    PERFORM yezzey_define_relation_offload_policy_internal_prepare_master(
        relation
    );

    -- non-partitioned relation
    PERFORM yezzey_define_relation_offload_policy_internal_seg(
        relation
    );
    PERFORM yezzey_define_relation_offload_policy_internal(
        relation
    );

    RETURN QUERY SELECT ('offloaded relation to external storage' )::TEXT;
END;
$$
LANGUAGE PLPGSQL;


CREATE FUNCTION yezzey_delete_obsolete(
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_collect_obsolete()
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_vacuum_garbage(
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_vacuum_relation(
    relation regclass,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


-- Add tablespace-level garbage vacuum wrapper

CREATE FUNCTION yezzey_vacuum_garbage_tablespace(
    tablespace OID,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_load_relation_internal(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_load_relation'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE FUNCTION yezzey_load_relation_internal_seg(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME', 'yezzey_load_relation'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION yezzey_load_relation(relation regclass)
RETURNS TABLE (status TEXT)
AS $$
BEGIN
    PERFORM yezzey_load_relation_internal_seg(relation);
    PERFORM yezzey_load_relation_internal(relation);

    RETURN QUERY SELECT 'relation loaded to local storage';
END;
$$
LANGUAGE PLPGSQL;
