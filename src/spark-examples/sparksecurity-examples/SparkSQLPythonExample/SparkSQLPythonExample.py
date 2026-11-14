# -*- coding:utf-8 -*-

import sys
from pyspark.sql import SparkSession


def contains(str1, substr1):
    if substr1 in str1:
        return True
    return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: SparkSQLPythonExample.py <file>")
        exit(-1)

    # 初始化SparkSession
    spark = SparkSession.builder.appName("CollectFemaleInfo").getOrCreate()

    # RDD转换为DataFrame
    inputPath = sys.argv[1]
    inputRDD = spark.read.text(inputPath).rdd.map(lambda r: r[0])\
        .map(lambda line: line.split(","))\
        .map(lambda dataArr: (dataArr[0], dataArr[1], int(dataArr[2])))\
        .collect()
    df = spark.createDataFrame(inputRDD)

    # 注册临时视图
    df.createOrReplaceTempView("FemaleInfoTable")

    # 执行SQL查询并显示结果
    FemaleTimeInfo = spark.sql("SELECT * FROM " +
               "(SELECT _1 AS Name,SUM(_3) AS totalStayTime FROM FemaleInfoTable " +
               "WHERE _2 = 'female' GROUP BY _1 )" +
               " WHERE totalStayTime >120").show()

    spark.stop()
