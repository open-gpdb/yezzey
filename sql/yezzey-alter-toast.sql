CREATE EXTENSION yezzey;
SET client_min_messages TO WARNING;
-- AO

CREATE TABLE y_a_toast_t(i int, t text) with (appendonly=true, orientation=column);

ALTER TABLE y_a_toast_t ALTER COLUMN t SET STORAGE EXTERNAL;

INSERT INTO y_a_toast_t VALUES(1, repeat('a', 320023));

SELECT * FROM yezzey_define_offload_policy('y_a_toast_t');

ALTER TABLE y_a_toast_t ADD COLUMN z text;

-- test multiple columns
ALTER TABLE y_a_toast_t ADD COLUMN z2 text, add column z3 text;

-- test multiple columns
ALTER TABLE y_a_toast_t ADD COLUMN z5 text, DROP column z2, add column z4 TEXT, ALTER COLUMN z3 SET STORAGE EXTERNAL;

INSERT INTO y_a_toast_t (z3) VALUES (repeat('a', 2323323));

SELECT spcname from pg_tablespace where oid = (select reltablespace from pg_class where relname='y_a_toast_t');

DROP TABLE y_a_toast_t;

\! echo AO TOAST ALTER test OK

DROP EXTENSION yezzey;
CHECKPOINT;
