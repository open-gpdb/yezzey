CREATE EXTENSION IF NOT EXISTS dblink;


create function yezzey_yproxy_restart()
returns setof text
as $$
    select * from dblink('host=localhost port=8432', 'STOP SYSTEM') as (cmd text)
$$ language sql execute on all segments;

CREATE TYPE yezzey_stats_tp AS (
    opType text,
    "size category" text,
    quantiles_0_001 double precision,
    quantiles_0_01 double precision,
    quantiles_0_1 double precision,
    quantiles_0_25 double precision,
    quantiles_0_5 double precision,
    quantiles_0_75 double precision,
    quantiles_0_9 double precision,
    quantiles_0_99 double precision,
    quantiles_0_999 double precision
);

create OR REPLACE function yezzey_stats() 
returns setof yezzey_stats_tp 
as $$ 
    select
        opType,
        "size category",
        quantiles_0_001::double precision * 10 ^ 9 / (1024 * 1024),
        quantiles_0_01::double precision * 10 ^ 9 / (1024 * 1024),
        quantiles_0_1::double precision * 10 ^ 9 / (1024 * 1024),
        quantiles_0_25::double precision * 10 ^ 9 / (1024 * 1024),
        quantiles_0_5::double precision * 10 ^ 9 / (1024 * 1024),
        quantiles_0_75::double precision * 10 ^ 9 / (1024 * 1024),
        quantiles_0_9::double precision * 10 ^ 9 / (1024 * 1024),
        quantiles_0_99::double precision * 10 ^ 9 / (1024 * 1024),
        quantiles_0_999::double precision * 10 ^ 9 / (1024 * 1024)
    from
        dblink('host=localhost port=8432', 'SHOW stats') 
    as (
        opType text, 
        "size category" text, 
        quantiles_0_001 text,
        quantiles_0_01 text,
        quantiles_0_1 text,
        quantiles_0_25 text,
        quantiles_0_5 text,
        quantiles_0_75 text,
        quantiles_0_9 text,
        quantiles_0_99 text,
        quantiles_0_999 text
    );
$$ language sql execute on all segments SECURITY DEFINER;

CREATE TYPE yezzey_yproxy_clients_tp AS (
    opType text, 
    client_id text,
    "byte offset" text,
    opstart text,
    xpath text
);

create function yezzey_yproxy_clients() 
returns setof yezzey_yproxy_clients_tp 
as $$ 
    select * 
    from 
        dblink('host=localhost port=8432', 'SHOW clients') 
    as (
        opType text, 
        client_id text,
        "byte offset" text,
        opstart text,
        xpath text
    );
$$ language sql execute on all segments;


CREATE TYPE yezzey_show_system_tp as (
    "start time" text, 
    "storage concurrency" text
);

create function yezzey_yproxy_system() 
returns setof yezzey_show_system_tp
as $$ 
    select * 
    from 
        dblink('host=localhost port=8432', 'SHOW stat_system') 
    as (
        "start time" text,
        "storage concurrency" text
    );
$$ language sql execute on all segments;
