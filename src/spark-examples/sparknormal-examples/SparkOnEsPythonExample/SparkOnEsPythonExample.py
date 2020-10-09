# -*- coding:utf-8 -*-
"""
[Note] Since PySpark doesn't support the Elasticsearch, this example calls Java code through Python.
"""
from py4j.java_gateway import java_import
from pyspark import SparkConf, SparkContext

# create the SparkConf
conf = SparkConf().setAppName("SparkOnEs")

# the configurations for Elasticsearch cluster
conf.set("es.nodes", "192.168.0.10:24100,192.168.0.11:24100,192.168.0.12:24100")
# when you specified in es.nodes, then es.port is not necessary
# conf.set("es.port", "24100")
conf.set("es.nodes.discovery", "true")
conf.set("es.index.auto.create", "true")
conf.set("es.internal.spark.sql.pushdown", "true")
conf.set("es.read.source.filter", "name,age,createdTime")
conf.set("es.scroll.size", "10000")
conf.set("es.input.max.docs.per.partition", "500000")

# create the SparkContext
sc = SparkContext(conf = conf)

# import the program's class to sc._jvm
java_import(sc._jvm, "com.huawei.bigdata.spark.examples.SparkOnEs")

# create an instance of the above class, then invoke its method to run the program
sc._jvm.SparkOnEs().main(sc._jsc)

# stop the SparkContext
sc.stop()
