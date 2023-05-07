/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.spark

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation

/**
 * Read and write hive example with kerberos
 *
 * @since 2021-01-25
 */
object Hive2Hive {


  def main(args: Array[String]): Unit = {

    runJob()

  }

  /**
   * Authentication in step one
   */
  def runJob(): Unit = {

    SparkUtil.login()
    UserGroupInformation.getLoginUser
      .doAs(
        new PrivilegedExceptionAction[Void]() {
          @throws[Exception]
          override def run: Void = {
            val sparkSession = SparkUtil.getSparkSession()
            val sql = "select sum(age),country from test.usr group by country"
            sparkSession.sql(sql).show()
            val sql2 = "insert into test.usr2 partition(country='CN') select user_id,user_name,age from test.usr where country='CN'"
            sparkSession.sql(sql2)
            sparkSession.close()
            null
          }
        }
      )
  }

}
