create table kafka_sink (
  uuid varchar(20),
  name varchar(10),
  age int,
  ts timestamp(3),
  p varchar(20)
) with (
  'connector' = 'kafka',
  'topic' = 'input2',
  'properties.bootstrap.servers' = '192.168.20.162:21005',
  'properties.group.id' = 'testGroup2',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);
create TABLE datagen_source (
  uuid varchar(20),
  name varchar(10),
  age int,
  ts timestamp(3),
  p varchar(20)
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1'
);
INSERT INTO
  kafka_sink
SELECT
  *
FROM
  datagen_source;