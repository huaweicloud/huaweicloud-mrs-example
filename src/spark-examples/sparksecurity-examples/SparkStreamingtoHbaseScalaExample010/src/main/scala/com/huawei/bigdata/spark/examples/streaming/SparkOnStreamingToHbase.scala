package com.huawei.bigdata.spark.examples.streaming

import java.io.IOException
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.huawei.hadoop.security.LoginUtil
import kafka.serializer.StringDecoder
import com.huawei.hadoop.security.KerberosUtil

/**
  * run streaming task and select table1 data from hbase, then update to table1
  */
object SparkOnStreamingToHbase {
  def main(args: Array[String]) {
    if (args.length < 3) {
      printUsage
    }
    val userPrincipal = "sparkuser"
    val userKeytabPath = "/opt/FIclient/user.keytab"
    val krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf"
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)

    val Array(checkPointDir, topics, brokers) = args
    val sparkConf = new SparkConf().setAppName("SparkOnStreamingToHbase")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // set CheckPoint dir
    if (!"nocp".equals(checkPointDir)) {
      ssc.checkpoint(checkPointDir)
    }

    val columnFamily = "cf"


    val topicArr = topics.split(",")
    val topicsSet = topicArr.toSet
    // val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val principalName: String = KerberosUtil.getKrb5DomainRealm()
    val kafkaParams = scala.collection.Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "testGroup",
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.kerberos.service.name" -> "kafka",
      "kerberos.domain.name" -> ("hadoop." + principalName),
      "auto.offset.reset" -> "latest",
      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    // map(_._1) is message key, map(_._2) is message value
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )
    val lines = messages.map(_.value)
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
