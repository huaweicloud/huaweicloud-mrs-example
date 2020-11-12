package com.huawei.bigdata.spark.examples.streaming

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

object HBaseStreamingBulkPutExample {
  def main(args: Array[String]): Unit = {
    if(args.length < 4){
      println("HBaseStreamingBulkPutExample {host} {port} {tableName} {columnFamily}")
      return
    }
    LoginUtil.loginWithUserKeytab()

    val host = args(0)
    val port = args(1)
    val tableName = args(2)
    val columnFamily = args(3)

    val conf = new SparkConf()
    conf.setAppName("HBase Streaming Bulk Put Example")
    val sc = new SparkContext(conf)

    try {

      val config = HBaseConfiguration.create()

      val hbaseContext = new HBaseContext(sc, config)
      val ssc = new StreamingContext(sc, Seconds(1))

      val lines = ssc.socketTextStream(host, port.toInt)
      hbaseContext.streamBulkPut[String](lines,
        TableName.valueOf(tableName),
        (putRecord) => {
          if (putRecord.length() > 0) {
            val put = new Put(Bytes.toBytes(putRecord))
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("foo"), Bytes.toBytes("bar"))
            put
          } else {
            null
          }
        })

      ssc.start()

      ssc.awaitTerminationOrTimeout(60000)

      ssc.stop(stopSparkContext = false)

    } finally {

      sc.stop()
    }


  }
}