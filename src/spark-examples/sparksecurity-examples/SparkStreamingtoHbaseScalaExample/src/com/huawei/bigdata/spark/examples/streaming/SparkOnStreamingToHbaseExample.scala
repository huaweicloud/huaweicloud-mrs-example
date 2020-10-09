/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.bigdata.spark.examples.streaming

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import com.huawei.hadoop.security.LoginUtil

/**
  * This is a simple example of putting kafka streamData into HBase
  * with the streamBulkPut function.
  */

object SparkOnStreamingToHbaseExample {
  def main(args: Array[String]) {
    if (args.length < 5) printUsage
    // in security cluster, do login
    LoginUtil.loginWithUserKeytab()

    val Array(checkPointDir, topics, brokers, tableName, columnFamily) = args
    val sparkConf = new SparkConf().setAppName("SparkOnStreamingToHbase_Scala")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val config = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, config)

    // set checkPointDir
    if (!"nocp".equals(checkPointDir)) ssc.checkpoint(checkPointDir)

    // set kafka params
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers
    )

    val topicArr = topics.split(",")
    val topicSet = topicArr.toSet
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).map(_._2)

    // use streamBulkPut function writes value to HBase table
    hbaseContext.streamBulkPut[String](lines,
      TableName.valueOf(tableName),
      (putRecord) => {
        if (putRecord.length > 0) {
          val put = new Put(Bytes.toBytes(putRecord))
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("cid"), Bytes.toBytes(putRecord))
          put
        } else null
      })

    ssc.start()
    ssc.awaitTermination()
  }

  private def printUsage {
    System.out.println("Usage: {checkPointDir} {topic} {brokerList} {tableName} {columnFamily}")
    System.exit(1)
  }
}