#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Usage: <bootstrapServers> <subscribeType> <topics> <protocol> <service> <domain> <checkpointLocation>")
        exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]
    protocol = sys.argv[4]
    service = sys.argv[5]
    domain = sys.argv[6]
    checkpointLocation = sys.argv[7]

    # 初始化sparkSession
    spark = SparkSession.builder.appName("SecurityKafkaWordCount").getOrCreate()
    spark.conf.set("spark.sql.streaming.checkpointLocation", checkpointLocation)

    # 创建表示来自kafka的input lines stream的DataFrame
    lines = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", bootstrapServers)\
    .option(subscribeType, topics)\
    .option("kafka.security.protocol", protocol)\
    .option("kafka.sasl.kerberos.service.name", service)\
    .option("kafka.kerberos.domain.name", domain)\
    .load()\
    .selectExpr("CAST(value AS STRING)")


    # 将lines切分为word
    words = lines.select(explode(split(lines.value, " ")).alias("word"))
    # 生成正在运行的word count
    wordCounts = words.groupBy("word").count()

    # 开始运行将running counts打印到控制台的查询
    query = wordCounts.writeStream\
    .outputMode("complete")\
    .format("console")\
    .start()

    query.awaitTermination()

