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
package com.huawei.bigdata.spark.examples.hbasecontext

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier
import org.apache.hadoop.hbase.util.Pair

/**
  * Run this example using command below:
  *
  * SPARK_HOME/bin/spark-submit --master local[2]
  * --class org.apache.hadoop.hbase.spark.example.hbasecontext.JavaHBaseBulkLoadExample
  * path/to/hbase-spark.jar {path/to/output/HFiles}
  *
  * This example will output put hfiles in {path/to/output/HFiles}, and user can run
  * 'hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles' to load the HFiles into table to
  * verify this example.
  * usage: hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles {path/to/output/HFiles} {tableName}
  */

object HBaseBulkLoadExample {
  def main(args: Array[String]):Unit = {
      if( args.length < 2){
        println ("HBaseBulkLoadExample {outputPath} {tableName}")
        return
      }
    val Array(outputPath, tableName) = args
    val columnFamily1 = "f1"
    val columnFamily2 = "f2"

    val sparkConf = new SparkConf ().setAppName ("JavaHBaseBulkLoadExample " + tableName)
    val sc = new SparkContext (sparkConf)

    try {
      val arr = Array("1," + columnFamily1 + ",b,1",
        "2," + columnFamily1 + ",a,2",
        "3," + columnFamily1 + ",b,1",
        "3," + columnFamily2 + ",a,1",
        "4," + columnFamily2 + ",a,3",
        "5," + columnFamily2 + ",b,3")

      val rdd = sc.parallelize(arr)
      val config = HBaseConfiguration.create
      config.set("hfile.compression", Compression.Algorithm.SNAPPY.getName())
      val hbaseContext = new HBaseContext(sc, config)
      hbaseContext.bulkLoad[String](rdd,
        TableName.valueOf(tableName),
        (putRecord) => {
          if (putRecord.length > 0) {

            val strArray = putRecord.split(",")
            val kfq = new KeyFamilyQualifier(Bytes.toBytes(strArray(0)), Bytes.toBytes(strArray(1)), Bytes.toBytes(strArray(2)))
            val pair = new Pair[KeyFamilyQualifier, Array[Byte]](kfq, Bytes.toBytes(strArray(3)))
            val ite = (kfq, Bytes.toBytes(strArray(3)))
            val itea = List(ite).iterator
            itea
          } else {
            null
          }
        },
        outputPath)
    } finally {
        sc.stop()
    }
  }
}
