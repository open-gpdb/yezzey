CREATE EXTENSION yezzey VERSION '1.0';

ALTER EXTENSION yezzey UPDATE TO '1.8.3';

INSERT INTO yezzey.yezzey_virtual_index (filenode,blkno,modcount) VALUES (1,1,1);

INSERT INTO yezzey.yezzey_virtual_index (filenode,blkno,modcount) VALUES (1,1,1);

TRUNCATE yezzey.yezzey_virtual_index;

DROP EXTENSION yezzey;

CHECKPOINT;