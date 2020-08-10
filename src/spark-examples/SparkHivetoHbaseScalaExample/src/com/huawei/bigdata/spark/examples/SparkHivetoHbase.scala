package com.huawei.bigdata.spark.examples

import java.io.{File, IOException}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SparkSession}
import com.huawei.hadoop.security.LoginUtil

/**
  * Get data from both hive and hbase, add values and update to hbase
  *
  * Before running the program, you need to add some data in hive and hbase:
  * 1. create table in hive with "spark-beeline": CREATE TABLE IF NOT EXISTS person (name STRING, account INT);
  * 2. insert data into hive with "spark-beeline": INSERT INTO TABLE person VALUES ("Lily", 5), ("Tom", 7);
  * 3. create table in hbase with "hbase shell": create 'table2', {NAME => 'cf'}
  * 4. insert data into hbase with "hbase shell": put 'table2', 'Lily', 'cf:cid', 11
  *
  * After running the program, you can do the following to check results:
  * 1. scan hbase table data with "hbase shell": scan 'table2', {COLUMNS => 'cf'}
  */
object SparkHivetoHbase {

  def main(args: Array[String]) {
    val hadoopConf: Configuration = new Configuration()
    if ("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))) {
      //security mode

      val userPrincipal = "sparkuser"
      val USER_KEYTAB_FILE = "user.keytab"
      val filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator
      val krbFile = filePath + "krb5.conf"
      val userKeyTableFile = filePath + USER_KEYTAB_FILE
      LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf)
    }

    // Obtain the data in the table through the Spark interface.
    val spark = SparkSession.builder()
      .appName("SparkHivetoHbase")
      .getOrCreate()

    val dataFrame = spark.sql("select name, account from person")

    // Traverse every Partition in the hive table and update the hbase table
    // If less data, you can use rdd.foreach()
    dataFrame.rdd.foreachPartition(x => hBaseWriter(x))

    spark.stop()
  }

  /**
    * write to hbase table in exetutor
    *
    * @param iterator partition data from hive table
    */
  def hBaseWriter(iterator: Iterator[Row]): Unit = {
    // read hbase
    val tableName = "table2"
    val columnFamily = "cf"
    val conf = HBaseConfiguration.create()
    var table: Table = null
    var connection: Connection = null

    try {
      connection = ConnectionFactory.createConnection(conf)
      table = connection.getTable(TableName.valueOf(tableName))

      val iteratorArray = iterator.toArray
      val rowList = new util.ArrayList[Get]()
      for (row <- iteratorArray) {
        // set the put condition
        val get = new Get(row.getString(0).getBytes)
        rowList.add(get)
      }

      // get data from hbase table
      val resultDataBuffer = table.get(rowList)

      // set data for hbase
      val putList = new util.ArrayList[Put]()
      for (i <- iteratorArray.indices) {
        // hbase row
        val resultData = resultDataBuffer(i)
        if (!resultData.isEmpty) {
          // get hiveValue
          var hiveValue = iteratorArray(i).getInt(1)

          // get hbaseValue by column Family and colomn qualifier
          val hbaseValue = Bytes.toString(resultData.getValue(columnFamily.getBytes, "cid".getBytes))
          val put = new Put(iteratorArray(i).getString(0).getBytes)

          // calculate result value
          val resultValue = hiveValue + hbaseValue.toInt

          // set data to put
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("cid"), Bytes.toBytes(resultValue.toString))
          putList.add(put)
        }
      }

      if (putList.size() > 0) {
        table.put(putList)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace();
    } finally {
      if (table != null) {
        try {
          table.close()
        } catch {
          case e: IOException =>
            e.printStackTrace();
        }
      }
      if (connection != null) {
        try {
          // Close the HBase connection.
          connection.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }
  }
}
