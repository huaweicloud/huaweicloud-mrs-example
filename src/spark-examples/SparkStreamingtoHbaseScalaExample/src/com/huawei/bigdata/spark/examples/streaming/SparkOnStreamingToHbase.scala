package com.huawei.bigdata.spark.examples.streaming

import java.io.{File, IOException}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.huawei.hadoop.security.LoginUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * run streaming task and select table1 data from hbase, then update to table1
  */
object SparkOnStreamingToHbase {
  val tableName = "table1"
  val columnFamily = "cf"
  val qualifier = "cid"

  case class Person(id: String, fee: Float)
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

    import ConsumerConfig._
    val kafkaParams = Map[String, Object](
      BOOTSTRAP_SERVERS_CONFIG -> brokers,
      VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      GROUP_ID_CONFIG -> "DemoConsumer",
      AUTO_OFFSET_RESET_CONFIG -> "latest",
      ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    val topicArr = topics.split(",")
    val topicSet = topicArr.toSet
    // map(_._1) is message key, map(_._2) is message value
    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )
      .map(_.value)

    lines.
      foreachRDD(rdd => {
      //run for partition
      rdd.map(line => (line.split(",")(0), line.split(",")(1).toFloat))
          .reduceByKey(_ + _)
          .map(pair => Person(pair._1, pair._2))
          .foreachPartition(iterator => hBaseWriter(iterator))
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * do write hbase in executor
    *
    * @param iterator  message
    */
  def hBaseWriter(iterator: Iterator[Person]): Unit = {
    val conf = HBaseConfiguration.create()
    var table: Table = null
    var connection: Connection = null

    try {
      connection = ConnectionFactory.createConnection(conf)
      table = connection.getTable(TableName.valueOf(tableName))
      val iteratorArray = iterator.toArray

      val rowList = new util.ArrayList[Get]()
      for (row <- iteratorArray) {
        val get = new Get(row.id.getBytes)
        rowList.add(get)
      }
      //get data from table1
      val resultDataBuffer: Array[Result] = table.get(rowList)
      val resultMap = resultDataBuffer.map(result => (Bytes.toString(result.getRow), result.getValue(columnFamily.getBytes, qualifier.getBytes())))
        .toMap

      //set data for table1
      val putList = new util.ArrayList[Put]()

      iteratorArray.foreach { person =>
        val oldTotal = java.lang.Float.valueOf(Bytes.toString(resultMap(person.id)))
        val newTotal = oldTotal + person.fee
        val put = new Put(person.id.getBytes)
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(String.valueOf(newTotal)))
        putList.add(put)
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
    println("Usage: SparkOnStreamingToHbase <checkPointDir> <topic> <brokerList>")
    System.exit(1)
  }
}
