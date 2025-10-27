CREATE EXTENSION yezzey;
SET client_min_messages TO WARNING;
-- AO

CREATE TABLE y_a_toast_t(i int, t text) with (appendonly=true);

ALTER TABLE y_a_toast_t ALTER COLUMN t SET STORAGE EXTERNAL;

INSERT INTO y_a_toast_t VALUES(1, repeat('a', 320023));

SELECT * FROM yezzey_define_offload_policy('y_a_toast_t');

ALTER TABLE y_a_toast_t ADD COLUMN z text;

DROP TABLE y_a_toast_t;

\! echo AO TOAST ALTER test OK

DROP EXTENSION yezzey;
CHECKPOINT;
