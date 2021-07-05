# -*- coding:utf-8 -*-
"""
【说明】
(1)由于pyspark不提供Hbase相关api,本样例使用Python调用Java的方式实现
(2)如果使用yarn-client模式运行,请确认Spark2x客户端Spark2x/spark/conf/spark-defaults.conf中
   spark.yarn.security.credentials.hbase.enabled参数配置为true
"""

import sys
from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: SparkOnStreamingToHbasePythonKafka10.py <checkPointDir> <topics> <brokers> <tableName>")
        exit(-1)

	# 创建SparkSession
    spark = SparkSession\
        .builder\
        .appName("SparkStreamingtoHbasePythonkafka10") \
        .getOrCreate()

	# 向sc._jvm中导入要运行的类
    java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.streaming.SparkOnStreamingToHbasePythonKafka10')

	# 创建类实例并调用方法
    spark._jvm.SparkOnStreamingToHbasePythonKafka10().streamingToHbase(spark._jsc, sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])