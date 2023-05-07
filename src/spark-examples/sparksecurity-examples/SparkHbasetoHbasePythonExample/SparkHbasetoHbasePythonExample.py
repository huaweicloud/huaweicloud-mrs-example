# -*- coding:utf-8 -*-
"""
【说明】
(1)由于pyspark不提供Hbase相关api,本样例使用Python调用Java的方式实现
(2)如果使用yarn-client模式运行,请确认Spark2x客户端Spark2x/spark/conf/spark-defaults.conf中
   spark.yarn.security.credentials.hbase.enabled参数配置为true
"""

from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

# 创建SparkSession，设置kryo序列化
spark = SparkSession\
        .builder\
        .appName("SparkHbasetoHbase") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator") \
        .getOrCreate()

# 向sc._jvm中导入要运行的类
java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.SparkHbasetoHbase')

# 创建类实例并调用方法
spark._jvm.SparkHbasetoHbase().hbasetohbase(spark._jsc)

# 停止SparkSession
spark.stop()

