
CREATE TABLE yezzey.yezzey_expire_hint
(
    x_path TEXT PRIMARY KEY,
    lsn pg_lsn
) WITH (appendonly=false);

CREATE INDEX yezzey_virtual_index_x_path ON yezzey.yezzey_virtual_index(x_path);
