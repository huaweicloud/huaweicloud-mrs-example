package com.huawei.bigdata.spark.examples

import java.util.Properties

import scala.collection.mutable
import scala.language.postfixOps

import kafka.producer.KeyedMessage

import com.huawei.spark.streaming.kafka.KafkaWriter._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}


/**
 * Exaple code to demonstrate the usage of dstream.writeToKafka API
 *
 * Parameter description:
 * <groupId> is the group ID for the consumer.
 * <brokers> is for bootstrapping and the producer will only use
 * <topic> is a kafka topic to consume from.
 */
object DstreamKafkaWriter {
  def main(args: Array[String]) {

    if (args.length != 3) {
      System.err.println("Usage: DstreamKafkaWriter <groupId> <brokers> <topic>")
      System.exit(1)
    }

    val Array(groupId, brokers, topic) = args
    val sparkConf = new SparkConf().setAppName("KafkaWriter")

    // Populate Kafka properties
    val kafkaParams = new Properties()
    kafkaParams.put("metadata.broker.list", brokers)
    kafkaParams.put("group.id", groupId)
    kafkaParams.put("auto.offset.reset", "smallest")

    // Create Spark Java streaming context
    val ssc = new StreamingContext(sparkConf, Milliseconds(500))

    // Populate data to write to kafka
    val sentData = Seq("kafka_writer_test_msg_01", "kafka_writer_test_msg_02",
      "kafka_writer_test_msg_03")

    // Create RDD queue
    val sent = new mutable.Queue[RDD[String]]()
    sent.enqueue(ssc.sparkContext.makeRDD(sentData))

    // Create Dstream with the data to be written
    val wStream = ssc.queueStream(sent)

    // Write to kafka
    wStream.writeToKafka(kafkaParams,
      (x: String) => new KeyedMessage[String, Array[Byte]](topic, x.getBytes))

    ssc.start()
    ssc.awaitTermination()
  }
}
