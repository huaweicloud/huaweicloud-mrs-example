package com.huawei.bigdata.spark.examples

import java.text.SimpleDateFormat
import java.util.{Date, HashMap}
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.network.util.JavaUtils

import scala.util.Random

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec) = args
    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    //
    val date = new Date
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>

        var value: String = null
        for (i <- 0 to Random.nextInt(6)) {
          value += (" " + Random.nextInt(10000))
        }
        date.setTime(System.currentTimeMillis())
        value += ("," + dataFormat.format(date))

        val message = new ProducerRecord[String, String](topic, null, value)
        producer.send(message)
      }

      Thread.sleep(10)
    }
  }
}
