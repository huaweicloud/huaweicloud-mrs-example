package com.huawei.spark.examples

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}


/**
  *
  * ./spark-submit --master yarn --deploy-mode client --keytab /opt/FIclient/user.keytab --principal sparkuser
  * --files /opt/example/B/hbase-site.xml --class com.huawei.spark.examples.SparkOnMultiHbase /opt/example/SparkOnMultiHbase-1.0.jar
  */
object SparkOnMultiHbase  {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkOnMultiHbaseExample")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.huawei.spark.examples.MyRegistrator")
    val sc = new SparkContext(conf)

    val tableName = "SparkOnMultiHbase"
    val clusterFlagList=List("B", "A")
    clusterFlagList.foreach{ item =>
      val hbaseConf = getConf( item )
      println(hbaseConf.get("hbase.zookeeper.quorum"))

      val hbaseUtil = new HbaseUtil(sc,hbaseConf)
      hbaseUtil.writeToHbase(tableName)
      hbaseUtil.readFromHbase(tableName)
    }
    sc.stop()
  }

  private def getConf(item:String):Configuration={
    val conf: Configuration = HBaseConfiguration.create()
    val url = "/opt" + File.separator + "example" + File.separator + item + File.separator + "hbase-site.xml"
    conf.addResource(new File(url).toURI.toURL)
    conf
  }
}