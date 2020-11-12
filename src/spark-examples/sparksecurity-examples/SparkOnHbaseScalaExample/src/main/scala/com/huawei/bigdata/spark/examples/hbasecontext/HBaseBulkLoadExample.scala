package com.huawei.bigdata.spark.examples.hbasecontext

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import com.huawei.hadoop.security.LoginUtil

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier
import org.apache.hadoop.hbase.util.Pair

object HBaseBulkLoadExample {
  def main(args: Array[String]):Unit = {
      if( args.length < 2){
        println ("JavaHBaseBulkLoadExample {outputPath} {tableName}");
        return
      }
      LoginUtil.loginWithUserKeytab
      val outputPath: String = args(0)
      val tableName: String = args(1)
      val columnFamily1: String = "f1"
      val columnFamily2: String = "f2"

      val sparkConf = new SparkConf ().setAppName ("JavaHBaseBulkLoadExample " + tableName);
      val sc = new SparkContext (sparkConf);

      try {
        val arr = Array(
            "1," + columnFamily1 + ",b,1"
          , "3," + columnFamily1 + ",a,2"
          , "3," + columnFamily1 + ",b,1"
          , "3," + columnFamily2 + ",a,1"
          ,"2," + columnFamily2 + ",a,3"
          ,"2," + columnFamily2 + ",b,3")

        val rdd = sc.parallelize(arr)
        val conf = HBaseConfiguration.create
        val hbaseContext = new HBaseContext(sc, conf)
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
      }finally {
        sc.stop ()
      }
  }
}
