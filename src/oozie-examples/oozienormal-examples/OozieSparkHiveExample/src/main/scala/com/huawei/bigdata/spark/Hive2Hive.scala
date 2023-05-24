/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.spark

/**
 * Read and write hive example
 *
 * @since 2021-01-25
 */
object Hive2Hive {


  def main(args: Array[String]): Unit = {

    runJob()

  }

  def runJob(): Unit = {
    val sparkSession = SparkUtil.getSparkSession()
    val sql = "select sum(age),country from test.usr group by country"
    sparkSession.sql(sql).show()
    val sql2 = "insert into test.usr2 partition(country='CN') select user_id,user_name,age from test.usr where country='CN'"
    sparkSession.sql(sql2)
    sparkSession.close()
  }

}
