create table datagen
(
    id    varchar,
    score int
) with (
      'connector' = 'datagen',
      'rows-per-second' = '5'
      );
CREATE table redis_sink
(
    id    varchar,
    score int,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'redis',
      'deploy-mode' = 'cluster',
      'cluster-address' = '192.168.64.138:22401,192.168.64.77:22401,192.168.64.63:22401',
      'data-type' = 'string',
      'namespace' = 'redis_table_2'
      );

INSERT INTO redis_sink
SELECT id,
       score
FROM datagen
