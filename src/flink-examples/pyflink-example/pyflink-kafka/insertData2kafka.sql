create table kafka_sink_table (
  age int,
  name varchar(10)
) with (
      'connector' = 'kafka',
      'topic' = 'test_source_topic',
      'properties.bootstrap.servers' = '192.168.20.162:21005',
      'properties.group.id' = 'test_group',
      'format' = 'json'
      );
create TABLE datagen_source_table (
  age int,
  name varchar(10)
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1'
      );
INSERT INTO
    kafka_sink_table
SELECT
    *
FROM
    datagen_source_table;