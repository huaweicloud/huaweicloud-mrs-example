/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.spark

import org.apache.spark.sql.SparkSession


/**
 * Utils for spark
 *
 * @since 2021-01-25
 */
object SparkUtil {

  def getSparkSession(): SparkSession = {
    val session = SparkSession
      .builder()
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()
    session
  }

}
