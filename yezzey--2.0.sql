\echo Use "CREATE EXTENSION yezzey" to load this file. \quit


-- since GP uses segment-file discovery technique
-- in can fail to remove some AO/AOCS relation files locally
-- in cases when table write happened after folloading
-- see ao_foreach_extent_file
-- 

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
-- this creates schema yezzey with pre-defined oid
-- 8001, virtual index relation, etc
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

CREATE FUNCTION yezzey_offload_relation_status_internal(reloid OID) 
RETURNS TABLE (reloid OID, segindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_offload_relation_status_modern'
VOLATILE
LANGUAGE C STRICT;


-- more detailed debug about relations file segments
CREATE FUNCTION yezzey_offload_relation_status_per_filesegment(reloid OID) 
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS 'MODULE_PATHNAME', 'yezzey_offload_relation_status_per_filesegment_modern'
VOLATILE
LANGUAGE C STRICT;


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

    -- SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE reloid = v_reloid;
    -- IF NOT FOUND THEN
    --     RAISE WARNING'relation %.% is not in offload metadata table', i_nspname, i_relname;
    -- END IF;

    RETURN QUERY SELECT 
        y.reloid, y.segindex, y.local_bytes, y.external_bytes, y.external_bloat_bytes
    FROM yezzey_offload_relation_status_internal(
        v_reloid
    ) y;
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


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
            y.offload_reloid, y.segindex, y.local_bytes, y.external_bytes, y.external_bloat_bytes
    FROM yezzey_offload_relation_status(
        'public',
        i_relname
    ) y;
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


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

    -- SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE reloid = v_reloid;

    -- IF NOT FOUND THEN
    --     RAISE WARNING'relation %.% is not in offload metadata table', i_nspname, i_relname;
    -- END IF;
 
    RETURN QUERY SELECT 
        y.reloid, y.segindex, y.segfileindex, y.local_bytes, y.external_bytes, y.external_bloat_bytes
    FROM yezzey_offload_relation_status_per_filesegment(
        v_reloid
    ) y;
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


CREATE FUNCTION yezzey_offload_relation_status_per_filesegment(i_relname TEXT) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, external_bytes BIGINT, external_bloat_bytes BIGINT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
BEGIN
 
    RETURN QUERY SELECT 
        y.offload_reloid, y.segindex, y.segfileindex, y.local_bytes, y.external_bytes, y.external_bloat_bytes
    FROM yezzey_offload_relation_status_per_filesegment(
        'public',
        i_relname
    ) y;
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;

-- even more detailed debug about relations file segments
CREATE FUNCTION yezzey_relation_describe_external_storage_structure_internal(reloid OID) 
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, external_storage_filepath TEXT, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;

CREATE FUNCTION yezzey_relation_describe_external_storage_structure(
    i_nspname TEXT, i_relname TEXT) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, segfileindex INTEGER, external_storage_filepath TEXT, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
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

    -- SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE reloid = v_reloid;

    -- IF NOT FOUND THEN
    --     RAISE WARNING'relation %.% is not in offload metadata table', i_nspname, i_relname;
    -- END IF;
 
    RETURN QUERY SELECT 
        *
    FROM yezzey_relation_describe_external_storage_structure_internal(
        v_reloid
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


CREATE FUNCTION yezzey_relation_describe_external_storage_structure(i_relname TEXT) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, segfileindex INTEGER, external_storage_filepath TEXT, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS $$
BEGIN
    RETURN QUERY SELECT 
        *
    FROM yezzey_relation_describe_external_storage_structure(
        'public',
        i_relname
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;

CREATE TABLE yezzey.auto_offload_relations(
    reloid OID,
    expire_date DATE
)
DISTRIBUTED REPLICATED;

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
yezzey_define_offload_policy(i_offload_nspname TEXT, i_offload_relname TEXT)
RETURNS TABLE (status TEXT)
AS $$
DECLARE
    v_tmprow OID;
    v_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = i_offload_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_offload_nspname);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'relation % is not found in pg_class', i_offload_relname;
    END IF;
    
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

    -- non-partitioned relation
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


CREATE FUNCTION
yezzey_define_offload_policy(i_offload_relname TEXT)
RETURNS TABLE (status TEXT)
AS $$
BEGIN
    RETURN QUERY SELECT yezzey_define_offload_policy('public', i_offload_relname);
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


-- New utilities & functions
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
    reloid OID,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_vacuum_relation(
    relname TEXT,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
AS $$ 
BEGIN
RETURN QUERY SELECT yezzey_vacuum_relation(relname::regclass::oid, confirm, crazyDrop);
END;
$$
LANGUAGE PLPGSQL;


CREATE FUNCTION yezzey_vacuum_garbage_relation(
    i_offload_nspname TEXT,
    i_offload_relname TEXT,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (status BOOLEAN)
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

    RETURN QUERY SELECT yezzey_vacuum_relation(
        v_reloid,confirm,crazyDrop
    );
END;
$$
LANGUAGE PLPGSQL;


CREATE FUNCTION
yezzey_vacuum_garbage_relation(
    i_offload_relname TEXT,
    confirm BOOLEAN DEFAULT FALSE,
    crazyDrop BOOLEAN DEFAULT FALSE)
RETURNS TABLE (status BOOLEAN)
AS $$
BEGIN
    RETURN QUERY SELECT yezzey_vacuum_garbage_relation('public', i_offload_relname, confirm, crazyDrop);
END;
$$
LANGUAGE PLPGSQL;
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
