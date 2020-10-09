# -*- coding:utf-8 -*-

import sys
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


def contains(str1, substr1):
    if substr1 in str1:
        return True
    return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: SparkSQLPythonExample.py <file>")
        exit(-1)

    # 初始化SparkSession和SQLContext
    sc = SparkSession.builder.appName("CollectFemaleInfo").getOrCreate()
    sqlCtx = SQLContext(sc)

    # RDD转换为DataFrame
    inputPath = sys.argv[1]
    inputRDD = sc.read.text(inputPath).rdd.map(lambda r: r[0])\
        .map(lambda line: line.split(","))\
        .map(lambda dataArr: (dataArr[0], dataArr[1], int(dataArr[2])))\
        .collect()
    df = sqlCtx.createDataFrame(inputRDD)

    # 注册表
    df.registerTempTable("FemaleInfoTable")

    # 执行SQL查询并显示结果
    FemaleTimeInfo = sqlCtx.sql("SELECT * FROM " +
               "(SELECT _1 AS Name,SUM(_3) AS totalStayTime FROM FemaleInfoTable " +
               "WHERE _2 = 'female' GROUP BY _1 )" +
               " WHERE totalStayTime >120").show()

    sc.stop()
