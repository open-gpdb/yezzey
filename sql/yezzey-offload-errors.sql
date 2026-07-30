-- Deterministic error-path and idempotency checks for the offload policy API.
-- Error cases are wrapped so only the raised message is printed (no volatile
-- CONTEXT stack), keeping the expected output stable across environments.

CREATE EXTENSION yezzey VERSION '1.0';

SET client_min_messages TO NOTICE;

-- 1) Offloading a relation that does not exist must raise a clear error.
DO $$
BEGIN
    PERFORM yezzey_define_offload_policy('no_such_relation_xyz');
    RAISE NOTICE 'unexpected: no error raised';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'caught: %', SQLERRM;
END;
$$;

-- 2) Offloading in a non-existent schema must also fail.
DO $$
BEGIN
    PERFORM yezzey_define_offload_policy('no_such_schema', 'no_such_relation_xyz');
    RAISE NOTICE 'unexpected: no error raised';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'caught: %', SQLERRM;
END;
$$;

CREATE TABLE offload_err_regaoty(i INT) WITH (appendonly=true) DISTRIBUTED BY (i);
INSERT INTO offload_err_regaoty SELECT * FROM generate_series(1, 10000);

-- 3) First offload succeeds.
-- The offload path emits per-segment progress NOTICEs whose logical EOF values
-- depend on how rows land across AO segments, so raise the message threshold to
-- keep the expected output stable.
SET client_min_messages TO WARNING;
SELECT yezzey_define_offload_policy('offload_err_regaoty');
RESET client_min_messages;

-- The relation must now be recorded as offloaded in the metadata catalog.
SELECT count(1) AS offloaded_rows
FROM yezzey.offload_metadata
WHERE reloid = 'offload_err_regaoty'::regclass AND relpolicy = 1;

-- 4) Re-offloading the same relation is a no-op and must emit a NOTICE
--    instead of erroring or duplicating metadata. Use terse verbosity so the
--    volatile PL/pgSQL CONTEXT stack is not printed alongside the NOTICE.
\set VERBOSITY terse
SELECT yezzey_define_offload_policy('offload_err_regaoty');
\set VERBOSITY default

-- Still exactly one metadata row after the repeated call.
SELECT count(1) AS offloaded_rows
FROM yezzey.offload_metadata
WHERE reloid = 'offload_err_regaoty'::regclass AND relpolicy = 1;

DROP TABLE offload_err_regaoty;

DROP EXTENSION yezzey;
CHECKPOINT;
