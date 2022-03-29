package com.huawei.bigdata.flink.examples

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object FlinkClickhouseScalaExample {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //连接Clickhouse的参数
    val driver = "ru.yandex.clickhouse.ClickHouseDriver"
    /* 安全模式使用https端口 url = "jdbc:clickhouse://192.168.0.183:21426/default?&ssl=true&sslmode=none" */
    val url = "jdbc:clickhouse://192.168.0.183:8123/default"
    val user = "default"
    val password = ""
    val sqlRead = "select * from students"
    val sqlWrite = "insert into students(name, city, id, age) values(?, ?, ?, ?)"
    /* create table students(name String, city String, id Int, age Int) ENGINE=MergeTree order by id */

    //调用写入Clickhouse的方法
    writeClickhouse(env, driver, url, user, password, sqlWrite)

    //从Clickhouse中读取数据
    readClickhouse(env, driver, url, user, password, sqlRead)
  }

  def readClickhouse(env: ExecutionEnvironment, driver: String, url: String, user: String, pwd: String, sql: String) : Unit = {
    val dataResult: DataSet[Row] = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(driver)
      .setDBUrl(url)
      .setUsername(user)
      .setPassword(pwd)
      .setQuery(sql)
      .setRowTypeInfo(new RowTypeInfo(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO))
      .finish())
    dataResult.print()
  }

  def writeClickhouse(env: ExecutionEnvironment, driver: String, url: String, user: String, pwd: String, sql: String) : Unit = {
    //要插入Clickhouse的数据
    val value: DataSet[Row] = env.fromElements(("Liming", "BeiJing", 0, 24))
      .map(t => {
        val row = new Row(4)
        row.setField(0, t._1)
        row.setField(1, t._2)
        row.setField(2, t._3)
        row.setField(3, t._4)
        row
      })

    if (value == null) {
      throw new RuntimeException("env.fromElements return null!")
    }

    value.output(JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername(driver)
      .setDBUrl(url)
      .setUsername(user)
      .setPassword(pwd)
      .setQuery(sql)
      .finish())

    env.execute("insert into clickhouse job")
    print("data write successfully")
  }
}
