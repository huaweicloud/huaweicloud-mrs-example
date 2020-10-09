package com.huawei.bigdata.spark.examples.streaming

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder


object SparkOnStreamingToHbaseExample {
  def main(args: Array[String]) {
    if (args.length < 5) {
      printUsage
    }

    val Array(checkPointDir, topics, brokers, tableName, columnFamily) = args
    val sparkConf = new SparkConf().setAppName("SparkOnStreamingToHbase_Scala")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val config = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, config)

    if (!"nocp".equals(checkPointDir)) {
      ssc.checkpoint(checkPointDir)
    }

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers
    )

    val topicArr = topics.split(",")
    val topicSet = topicArr.toSet
    val mm = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).map(_._2)

    hbaseContext.streamBulkPut[String](lines,
      TableName.valueOf(tableName),
      (putRecord) => {
        if (putRecord.length > 0) {
          val put = new Put(Bytes.toBytes(putRecord))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("cid"), Bytes.toBytes(putRecord))
          put
        } else {
          null
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }

  private def printUsage {
    System.out.println("Usage: {checkPointDir} {topic} {brokerList} {tableName} {columnFamily}")
    System.exit(1)
  }
}