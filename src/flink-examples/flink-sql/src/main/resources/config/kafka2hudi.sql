create table kafka_source
(
    uuid varchar(20),
    name varchar(10),
    age  int,
    ts   timestamp(3),
    p    varchar(20)
) with (
      'connector' = 'kafka',
      'topic' = 'input2',
      'properties.bootstrap.servers' = '192.168.37.112:21005,192.168.37.132:21005,192.168.37.196:21005',
      'properties.group.id' = 'testGroup2',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'json'
      );
create table hudi_sink
(
    uuid varchar(20),
    name varchar(10),
    age  int,
    ts   timestamp(3),
    p    varchar(20)
) with (
      'connector' = 'hudi',
      'path' = 'hdfs://hacluster/tmp/hudi/flink_stream_mor1',
      'table.type' = 'MERGE_ON_READ',
      'hoodie.datasource.write.recordkey.field' = 'uuid',
      'hoodie.datasource.query.type' = 'snapshot',
      'write.precombine.field' = 'uuid',
      'write.tasks' = '1',
      'write.index_bootstrap.tasks' = '1',
      'write.bucket_assign.tasks' = '1',
      'hoodie.datasource.write.hive_style_partitioning' = 'true',
      'read.streaming.enabled' = 'true',
      'read.streaming.check-interval' = '5',
      'compaction.delta_commits' = '3',
      'compaction.async.enabled' = 'false',
      'compaction.schedule.enabled' = 'true',
      'clean.async.enabled' = 'false'
      );
insert into hudi_sink
select *
from kafka_source;