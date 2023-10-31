package com.huawei.bigdata.spark.examples

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.clickhouse.jdbc.{ClickHouseDriver, ClickHouseStatement}

/**
 * SparkOnClickHouseScalaExample
 */

object SparkOnClickHouseExample {

  val DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Missing parameters, need ckJdbcUrl, ckDBName, ckTableName")
      System.exit(0)
    }

    val spark =  SparkSession
      .builder()
      .config(getSparkConf)
      .appName("SparkOnClickHouseScalaExample")
      .enableHiveSupport()
      .getOrCreate()

    val clickHouseJdbcUrl = args(0)
    val clickHouseDB = args(1)
    val clickHouseTable = args(2)
    val user = args(3)

    val props = new Properties()
    props.put("driver", DRIVER)
    props.put("user", user);
    val ckDriver = new ClickHouseDriver
    val ckConnect = ckDriver.connect(clickHouseJdbcUrl, props)
    val ckStatement = ckConnect.createStatement()

    val allDBs = s"show databases"
    val allDatabases = ckStatement.executeQuery(allDBs)
    while (allDatabases.next()) {
      val dbName = allDatabases.getString(1)
      println("dbName: " + dbName)
    }

    val startTime = System.currentTimeMillis()
    clickHouseExecute(ckStatement, clickHouseDB, clickHouseTable)
    println("[Elapsed]" + (System.currentTimeMillis() - startTime))

    val ckData = spark.read
      .format("jdbc")
      .option("url", clickHouseJdbcUrl)
      .option("user", user)
      .option("driver", DRIVER)
      .option("dbtable", clickHouseDB + "." + clickHouseTable)
      .load()

    ckData.show()

    ckData.registerTempTable("ckTempTable")

    val newCkData = spark.sql("select EventDate,cast(id+20 as decimal(20,0)) as id,name,age,address from ckTempTable")

    newCkData.show()
    newCkData.write.mode("Append").jdbc(clickHouseJdbcUrl, clickHouseDB+"." + clickHouseTable, props)

    spark.stop()
  }

  def getSparkConf: SparkConf = {
    val conf = new SparkConf().setAppName("SparkOnClickHouseScalaExample")
    conf.set("spark.driver.maxResultSize", "1G")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.task.maxFailures", "20")
    conf.set("spark.default.parallelism", "50")
    conf.set("spark.executor.momory", "1G")
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.cores", "2")
    conf.set("spark.sql.shuffle.partitions", "50")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf
  }

  def clickHouseExecute(ckStatement: ClickHouseStatement, clickHouseDB: String, clickHouseTable: String) {
    val createDB = s"CREATE DATABASE ${clickHouseDB} ON CLUSTER default_cluster"

    val createTable = s"CREATE TABLE ${clickHouseDB}.${clickHouseTable} ON CLUSTER default_cluster " +
      "(`EventDate` DateTime,`id` UInt64,`name` String,`age` UInt8,`address` String)" +
      s"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/${clickHouseDB}/${clickHouseTable}', '{replica}')" +
      "PARTITION BY toYYYYMM(EventDate) ORDER BY id"

    val insertData = s"insert into ${clickHouseDB}.${clickHouseTable} (id, name,age,address) values (1, 'zhangsan',17,'xian'), (2, 'lisi',36,'beijing')"

    ckStatement.execute(createDB)
    ckStatement.execute(createTable)
    ckStatement.execute(insertData)
  }
}
