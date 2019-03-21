package com.huawei.bigdata.spark.examples.streaming

import java.io.{File, IOException}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.huawei.hadoop.security.LoginUtil
import kafka.serializer.StringDecoder

/**
  * run streaming task and select table1 data from hbase, then update to table1
  */
object SparkOnStreamingToHbase {
  def main(args: Array[String]) {
    if (args.length < 3) {
      printUsage
    }
    val hadoopConf: Configuration  = new Configuration()
    if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))) {
      //security mode

      val userPrincipal = "sparkuser"
      val USER_KEYTAB_FILE = "user.keytab"
      val filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator
      val krbFile = filePath + "krb5.conf"
      val userKeyTableFile = filePath + USER_KEYTAB_FILE
      LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf)
    }

    val Array(checkPointDir, topics, brokers) = args
    val sparkConf = new SparkConf().setAppName("SparkOnStreamingToHbase")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // set CheckPoint dir
    if (!"nocp".equals(checkPointDir)) {
      ssc.checkpoint(checkPointDir)
    }

    val columnFamily = "cf"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers
    )

    val topicArr = topics.split(",")
    val topicSet = topicArr.toSet
    // map(_._1) is message key, map(_._2) is message value
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).map(_._2)
    lines.foreachRDD(rdd => {
      //run for partition
      rdd.foreachPartition(iterator => hBaseWriter(iterator, columnFamily))
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * do write hbase in executor
    *
    * @param iterator  message
    * @param columnFamily columnFamily
    */
  def hBaseWriter(iterator: Iterator[String], columnFamily: String): Unit = {
    val conf = HBaseConfiguration.create()
    var table: Table = null
    var connection: Connection = null

    try {
      connection = ConnectionFactory.createConnection(conf)
      table = connection.getTable(TableName.valueOf("table1"))

      val iteratorArray = iterator.toArray
      val rowList = new util.ArrayList[Get]()
      for (row <- iteratorArray) {
        val get = new Get(row.getBytes)
        rowList.add(get)
      }
      //get data from table1
      val resultDataBuffer = table.get(rowList)

      //set data for table1
      val putList = new util.ArrayList[Put]()
      for (i <- 0 until iteratorArray.size) {
        val row = iteratorArray(i)
        val resultData = resultDataBuffer(i)
        if (!resultData.isEmpty) {
          // get value by column Family and colomn qualifier
          val aCid = Bytes.toString(resultData.getValue(columnFamily.getBytes, "cid".getBytes))
          val put = new Put(Bytes.toBytes(row))

          // calculate result value
          val resultValue = row.toInt + aCid.toInt
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("cid"), Bytes.toBytes(resultValue.toString))
          putList.add(put)
        }
      }

      if (putList.size() > 0) {
        table.put(putList)
      }
    }catch {
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

  private def printUsage {
    System.out.println("Usage: {checkPointDir} {topic} {brokerList}")
    System.exit(1)
  }
}
