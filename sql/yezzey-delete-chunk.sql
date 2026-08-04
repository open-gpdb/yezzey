
CREATE EXTENSION yezzey VERSION '1.0';
ALTER EXTENSION yezzey UPDATE TO '1.8.8';

SET client_min_messages TO WARNING;

CREATE TABLE delete_single_chunk_yaot(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
SELECT yezzey_define_offload_policy('delete_single_chunk_yaot');

INSERT INTO delete_single_chunk_yaot SELECT * FROM generate_series(1, 1000);

SELECT count(1) AS initial_chunks
    FROM yezzey_relation_describe_external_storage_structure('delete_single_chunk_yaot');

DO $$
DECLARE
    one_path TEXT;
    before_count INT;
    after_count  INT;
BEGIN
    SELECT count(1) INTO before_count
        FROM yezzey_relation_describe_external_storage_structure('delete_single_chunk_yaot');

    SELECT external_storage_filepath INTO one_path
        FROM yezzey_relation_describe_external_storage_structure('delete_single_chunk_yaot')
        LIMIT 1;

    PERFORM yezzey_delete_chunk(one_path);

    SELECT count(1) INTO after_count
        FROM yezzey_relation_describe_external_storage_structure('delete_single_chunk_yaot');

    IF after_count <> before_count - 1 THEN
        RAISE EXCEPTION 'expected % chunks after single delete, got %',
            before_count - 1, after_count;
    END IF;
END;
$$;

SELECT 'single chunk delete ok' AS result;

DROP TABLE delete_chunk_aot;
DROP TABLE delete_single_chunk_yaot;

DROP EXTENSION yezzey;
CHECKPOINT;
