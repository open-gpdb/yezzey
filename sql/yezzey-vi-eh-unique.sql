CREATE EXTENSION yezzey;

SET allow_system_table_mods TO on;

INSERT INTO yezzey.yezzey_virtual_index (filenode,blkno,modcount) VALUES (1,1,1);

INSERT INTO yezzey.yezzey_virtual_index (filenode,blkno,modcount) VALUES (1,1,1);

TRUNCATE yezzey.yezzey_virtual_index;

DROP EXTENSION yezzey;

CHECKPOINT;
