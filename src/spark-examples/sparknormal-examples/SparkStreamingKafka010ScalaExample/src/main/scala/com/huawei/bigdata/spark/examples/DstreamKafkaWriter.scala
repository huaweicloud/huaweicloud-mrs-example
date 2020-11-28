package com.huawei.bigdata.spark.examples

import scala.collection.mutable
import scala.language.postfixOps

import com.huawei.spark.streaming.kafka010.KafkaWriter._
import org.apache.kafka.clients.producer.ProducerRecord
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
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest"
    )

    // Create Spark streaming context
    val ssc = new StreamingContext(sparkConf, Milliseconds(500));

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
      (x: String) => new ProducerRecord[String, Array[Byte]](topic, x.getBytes))

    ssc.start()
    ssc.awaitTermination()
  }
}
