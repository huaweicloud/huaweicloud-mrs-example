package com.huawei.bigdata.spark.examples

import java.io.File

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.huawei.hadoop.security.LoginUtil

/**
  * Consumes messages from one or more topics in Kafka.
  * <batchTime> is the Spark Streaming batch duration in seconds.
  * <topics> is a list of one or more kafka topics to consume from
  * <brokers> is for bootstrapping and the producer will only use it for getting metadata
  */

object FemaleInfoCollectionPrint {
  def main(args: Array[String]) {
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

    val Array(checkPointDir, batchTime, topics, brokers) = args
    val batchDuration = Seconds(batchTime.toInt)

    // Create a Streaming startup environment.
    val sparkConf = new SparkConf()
    sparkConf.setAppName("DataSightStreamingExample")
    val ssc = new StreamingContext(sparkConf, batchDuration)

    // Configure the CheckPoint directory for the Streaming.
    ssc.checkpoint(checkPointDir)

    // Get the list of topic used by kafka
    val topicsSet = topics.split(",").toSet

    // Create direct kafka stream with brokers and topics
    // Receive data from the Kafka and generate the corresponding DStream
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)

    // Obtain field properties in each row.
    val records = lines.map(getRecord)
    // Filter data about the time that female netizens spend online
    val femaleRecords = records.filter(_._2 == "female")
      .map(x => (x._1, x._3))
    // Filter data about users whose consecutive online duration exceeds the threshold and export the result
    femaleRecords.filter(_._2 > 30).print()

    // The Streaming system starts.
    ssc.start()
    ssc.awaitTermination()
  }

  // get enums of record
  def getRecord(line: String): (String, String, Int) = {
    val elems = line.split(",")
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    (name, sexy, time)
  }
}
