CREATE TABLE hudi_source
(
    uuid varchar(20),
    name varchar(10),
    age  int,
    ts   timestamp(3),
    p    varchar(20)
) WITH (
      'connector' = 'hudi',
      'path' = 'hdfs://hacluster/tmp/hudi/flink_stream_mor1',
      'table.type' = 'MERGE_ON_READ',
      'hoodie.datasource.write.recordkey.field' = 'uuid',
      'hoodie.datasource.query.type' = 'snapshot',
      'write.precombine.field' = 'uuid',
      'write.tasks' = '1',
      'read.tasks' = '1',
      'write.index_bootstrap.tasks' = '1',
      'write.bucket_assign.tasks' = '1',
      'hoodie.datasource.write.hive_style_partitioning' = 'true',
      'read.streaming.enabled' = 'true',
      'read.streaming.check-interval' = '5',
      'compaction.delta_commits' = '3',
      'compaction.async.enabled' = 'false',
      'compaction.schedule.enabled' = 'true',
      'clean.async.enabled' = 'false',
      'read.start-commit' = 'earliest'
      );
CREATE TABLE print_sink
(
    uuid varchar(20),
    name varchar(10),
    age  int,
    ts   timestamp(3),
    p    varchar(20)
) WITH ('connector' = 'print');
insert into print_sink
select *
from hudi_source;