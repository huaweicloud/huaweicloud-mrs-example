package com.huawei.bigdata.spark.examples

import java.text.SimpleDateFormat
import java.util.{Date, HashMap}
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.network.util.JavaUtils

import scala.util.Random

// Produces some adEvent.
object KafkaADEventProducer {

  def main(args: Array[String]) {
    if (args.length < 9) {
      System.err.println("Usage: KafkaADEventProducer <metadataBrokerList> <totalTime> " +
        "<aheadTime> <reqTopic> <reqCount> <showTopic> <maxShowDelay> <clickTopic> <maxClickDelay>")
      System.exit(1)
    }

    val Array(brokers, totalTime, aheadTime, reqTopic, reqCount,
    showTopic, maxShowDelay, clickTopic, maxClickDelay) = args

    val sleepTime = 30
    val aheadMillis = JavaUtils.timeStringAs(aheadTime, TimeUnit.MILLISECONDS)
    val roundCount = JavaUtils.timeStringAs(totalTime, TimeUnit.SECONDS) / sleepTime
    val maxShowDelayMills = JavaUtils.timeStringAs(maxShowDelay, TimeUnit.MILLISECONDS)
    val maxClickDelayMills = JavaUtils.timeStringAs(maxClickDelay, TimeUnit.MILLISECONDS)
    val reqPerRound = reqCount.toInt / roundCount.toInt

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, reqPerRound.toString)

    val producer = new KafkaProducer[String, String](props)

    val date = new Date
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // Send some messages
    for (i <- 1 until roundCount.toInt) {
      for (j <- 1 until reqPerRound) {
        val adID = java.util.UUID.randomUUID.toString
        val reqTime = System.currentTimeMillis() - Random.nextInt(aheadMillis.toInt)
        date.setTime(reqTime)
        val reqEvent = new ProducerRecord[String, String](reqTopic, null, adID + "^" + dataFormat.format(date))
        producer.send(reqEvent)

        for(j <- 0 until Random.nextInt(5)) {
          val showID = java.util.UUID.randomUUID.toString
          val showTime = reqTime + Random.nextInt(maxShowDelayMills.toInt)
          date.setTime(showTime)
          val showEvent = new ProducerRecord[String, String](showTopic, null,
            adID + "^" + showID + "^" + dataFormat.format(date))
          producer.send(showEvent)

          for(j <- 0 until Random.nextInt(5)) {
            val clickTime = showTime + Random.nextInt(maxClickDelayMills.toInt)
            date.setTime(clickTime)
            val clickEvent = new ProducerRecord[String, String](clickTopic, null,
              adID + "^" + showID + "^" + dataFormat.format(date))
            producer.send(clickEvent)
          }
        }

      }

      Thread.sleep(sleepTime * 1000)
    }
  }

}
