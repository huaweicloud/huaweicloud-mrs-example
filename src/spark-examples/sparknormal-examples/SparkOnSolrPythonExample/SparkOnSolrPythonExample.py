# -*- coding:utf-8 -*-
"""
【说明】
(1)由于pyspark不提供Solr相关api,本样例使用Python调用Java的方式实现
"""
from py4j.java_gateway import java_import
from pyspark import SparkContext

# 创建SparkContext
spark = SparkContext(appName='SparkOnSolr')

# 向sc._jvm中导入要运行的类
java_import(spark._jvm, 'com.huawei.bigdata.spark.examples.SparkOnSolr')

# 创建类实例并调用方法
spark._jvm.SparkOnSolr().sparkonsolr(spark._jsc)

# 停止SparkContext
spark.stop()
