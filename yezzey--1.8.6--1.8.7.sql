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

DROP FUNCTION yezzey_define_relation_offload_policy_internal_prepare_master(OID);

CREATE FUNCTION yezzey_define_relation_offload_policy_internal_prepare_master(reloid OID)
RETURNS TABLE (status BOOLEAN)
AS 'MODULE_PATHNAME','yezzey_define_relation_offload_policy_internal_prepare'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

DROP FUNCTION yezzey_define_offload_policy(TEXT, TEXT, offload_policy);

CREATE FUNCTION
yezzey_define_offload_policy(i_offload_nspname TEXT, i_offload_relname TEXT, i_policy offload_policy DEFAULT 'remote_always')
RETURNS TABLE (status TEXT)
AS $$
DECLARE
    v_tmprow OID;
    v_reloid OID;
    v_par_reloid OID;
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
        RAISE NOTICE 'relation % already offloaded', i_offload_relname;
	RETURN;
    END IF;

    PERFORM yezzey_define_relation_offload_policy_internal_prepare(
        v_reloid
    );

    PERFORM yezzey_define_relation_offload_policy_internal_prepare_master(
        v_reloid
    );

    SELECT parrelid 
         FROM pg_partition
    INTO v_par_reloid 
    WHERE parrelid = v_reloid;

    IF NOT FOUND THEN
        -- non-partitioned relation
        PERFORM yezzey_define_relation_offload_policy_internal_seg(
            v_reloid
        );
        PERFORM yezzey_define_relation_offload_policy_internal(
            v_reloid
        );
    ELSE 

         FOR v_tmprow IN 
             SELECT (i_offload_nspname||'.'||partitiontablename)::regclass::oid FROM pg_partitions WHERE schemaname = i_offload_nspname AND tablename = i_offload_relname
         LOOP

             RAISE NOTICE 'offloading partition oid %', v_tmprow;
             -- offload each part
             PERFORM yezzey_define_relation_offload_policy_internal_seg(
                 v_tmprow
             );
             PERFORM yezzey_define_relation_offload_policy_internal(
                 v_tmprow
             );
         END LOOP;

    END IF;


    RETURN QUERY SELECT ('offloaded relation ' || i_offload_nspname ||'.'|| i_offload_relname || ' to external storage' )::TEXT;
END;
$$
LANGUAGE PLPGSQL;

DROP FUNCTION yezzey_define_offload_policy(TEXT, offload_policy);

CREATE FUNCTION
yezzey_define_offload_policy(i_offload_relname TEXT, i_policy offload_policy DEFAULT 'remote_always')
RETURNS TABLE (status TEXT)
AS $$
BEGIN
    RETURN QUERY SELECT yezzey_define_offload_policy('public', i_offload_relname, i_policy);
END;
$$
LANGUAGE PLPGSQL;
