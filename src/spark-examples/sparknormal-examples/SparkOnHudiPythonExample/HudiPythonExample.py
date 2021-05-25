# -*- coding:utf-8 -*-

#Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.


import sys
from pyspark.sql import SparkSession



basePath = sys.argv[1]
tableName = sys.argv[2]
print(basePath)
spark = SparkSession \
    .builder \
    .appName("HudiPythonExample") \
    .getOrCreate()

sc = spark.sparkContext
dataGen = sc._jvm.org.apache.hudi.QuickstartUtils.DataGenerator()

#insert
inserts = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateInserts(10))
df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.operation': 'insert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}
df.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)
tripsSnapshotDF = spark. \
    read. \
    format("hudi"). \
    load(basePath + "/*/*/*/*")
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()

#update
updates = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateUpdates(10))
df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
df.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(basePath)
# reload data
spark. \
    read. \
    format("hudi"). \
    load(basePath + "/*/*/*/*"). \
    createOrReplaceTempView("hudi_trips_snapshot")
commits = list(map(lambda row: row[0], spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").limit(50).collect()))
beginTime = commits[len(commits) - 2] # commit time we are interested in

# incrementally query data
incremental_read_options = {
    'hoodie.datasource.query.type': 'incremental',
    'hoodie.datasource.read.begin.instanttime': beginTime,
}

tripsIncrementalDF = spark.read.format("hudi"). \
    options(**incremental_read_options). \
    load(basePath)
tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()
# Represents all commits > this time.
beginTime = "000"
endTime = commits[len(commits) - 2]

# query point in time data
point_in_time_read_options = {
    'hoodie.datasource.query.type': 'incremental',
    'hoodie.datasource.read.end.instanttime': endTime,
    'hoodie.datasource.read.begin.instanttime': beginTime
}

tripsPointInTimeDF = spark.read.format("hudi"). \
    options(**point_in_time_read_options). \
    load(basePath)

tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")
spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_trips_point_in_time where fare > 20.0").show()


# 获取记录总数
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
# 拿到两条将被删除的记录
ds = spark.sql("select uuid, partitionpath from hudi_trips_snapshot").limit(2)

# 执行删除
hudi_delete_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.operation': 'delete',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

from pyspark.sql.functions import lit
deletes = list(map(lambda row: (row[0], row[1]), ds.collect()))
df = spark.sparkContext.parallelize(deletes).toDF(['uuid', 'partitionpath']).withColumn('ts', lit(0.0))
df.write.format("hudi"). \
    options(**hudi_delete_options). \
    mode("append"). \
    save(basePath)

# 像之前一样运行查询
roAfterDeleteViewDF = spark. \
    read. \
    format("hudi"). \
    load(basePath + "/*/*/*/*")
roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
# 应返回 (total - 2) 条记录
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
spark.sql("select uuid, partitionpath from hudi_trips_snapshot").show()

spark.stop()