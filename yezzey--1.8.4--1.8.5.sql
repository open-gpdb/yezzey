/* Bloat files deletion */
reindex index yezzey.offload_metadata_indx;
reindex index yezzey.yezzey_virtual_index_idx;

CREATE FUNCTION yezzey_delete_obsolete(
    crazyDrop BOOLEAN DEFAULT FALSE
) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE FUNCTION yezzey_collect_obsolete(
) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;