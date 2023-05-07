package com.huawei.bigdata.spark.examples

import java.io.{File, FileInputStream}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object ThriftServerQueriesTest {
  def main(args: Array[String]): Unit = {
    val config: Configuration = new Configuration()
    config.addResource(new Path(args(0)))
    val zkUrl = config.get("spark.deploy.zookeeper.url")

    var fileInfo: Properties = null
    val sparkConfPath = args(1)
    var fileInputStream: FileInputStream = null

    try {
      fileInfo = new Properties
      val propertiesFile: File = new File(sparkConfPath)
      fileInputStream = new FileInputStream(propertiesFile)
      //Load the "spark-defaults.conf" configuration file
      fileInfo.load(fileInputStream)

    } finally {
      if (fileInputStream != null)
        fileInputStream.close()
    }

    var zkNamespace: String = null
    zkNamespace = fileInfo.getProperty("spark.thriftserver.zookeeper.namespace")
    //Remove redundant characters from configuration items
    if (zkNamespace != null) zkNamespace = zkNamespace.substring(1)

    val sb = new StringBuilder("jdbc:hive2://"
      + zkUrl
      + "/;serviceDiscoveryMode=zooKeeper;"
      + "zooKeeperNamespace="
      + zkNamespace + ";");
    val url = sb.toString()

    val sqlList = new ArrayBuffer[String]
    sqlList += "CREATE TABLE IF NOT EXISTS CHILD (NAME STRING, AGE INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    sqlList += "LOAD DATA LOCAL INPATH '/home/data' INTO TABLE CHILD"
    sqlList += "SELECT * FROM child"
    sqlList += "DROP TABLE child"

    executeSql(url, sqlList.toArray)
  }

  def executeSql(url: String, sqls: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance()
    var connection: Connection = null
    var statement: PreparedStatement = null
    try {
      connection = DriverManager.getConnection(url)
      for (sql <- sqls) {
        println(s"---- Begin executing sql: $sql ----")
        statement = connection.prepareStatement(sql)

        val result = statement.executeQuery()

        val resultMetaData = result.getMetaData
        val colNum = resultMetaData.getColumnCount
        for (i <- 1 to colNum) {
          print(resultMetaData.getColumnLabel(i) + "\t")
        }
        println()

        while (result.next()) {
          for (i <- 1 to colNum) {
            print(result.getString(i) + "\t")
          }
          println()
        }
        println(s"---- Done executing sql: $sql ----")
      }
    } finally {
      if (null != statement) {
        statement.close()
      }

      if (null != connection) {
        connection.close()
      }
    }
  }
}
