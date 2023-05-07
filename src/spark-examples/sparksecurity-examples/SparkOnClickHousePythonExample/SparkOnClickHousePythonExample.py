# -*- coding:utf-8 -*-
"""
【说明】
(1)由于pyspark不提供SparkOnClickHouse相关api,本样例使用Python调用Java的方式实现
"""

import sys

from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

# 创建SparkSession
spark = SparkSession \
        .builder \
        .appName("SparkOnClickHousePythonExample") \
        .getOrCreate()

# 向sc._jvm中导入要运行的类
java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.SparkOnClickHouseExample')

# 创建类实例并调用方法
spark._jvm.SparkOnClickHouseExample().sparkOnClickHouse(spark._jsc, sys.argv)

# 停止SparkSession
spark.stop()
