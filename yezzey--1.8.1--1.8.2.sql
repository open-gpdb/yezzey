reindex index yezzey.offload_metadata_indx;

CREATE TABLE yezzey.yezzey_virtual_index_stale AS 
    SELECT * FROM yezzey.yezzey_virtual_index LIMIT 0;

CREATE TABLE yezzey.offload_metadata_stale AS
    SELECT * FROM yezzey.offload_metadata LIMIT 0;

CREATE FUNCTION
yezzey.yezzey_fixup_stale_metadata() RETURNS VOID
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

$$ LANGUAGE SQL
EXECUTE ON ALL SEGMENTS;