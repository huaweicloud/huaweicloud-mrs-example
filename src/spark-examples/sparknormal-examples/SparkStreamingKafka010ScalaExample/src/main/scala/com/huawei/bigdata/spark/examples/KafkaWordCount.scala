package com.huawei.bigdata.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

/**
  * Consumes messages from one or more topics in Kafka.
  * <checkPointDir> is the Spark Streaming checkpoint directory.
  * <brokers> is for bootstrapping and the producer will only use it for getting metadata
  * <topics> is a list of one or more kafka topics to consume from
  * <batchTime> is the Spark Streaming batch duration in seconds.
  */
object KafkaWordCount {

  def main(args: Array[String]) {
    val ssc = createContext(args)

    //The Streaming system starts.
    ssc.start()
    ssc.awaitTermination()
  }

  def createContext(args : Array[String]) : StreamingContext = {
    val Array(checkPointDir, brokers, topics, batchTime) = args

    // Create a Streaming startup environment.
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(batchTime.toLong))

    //Configure the CheckPoint directory for the Streaming.
    //This parameter is mandatory because of existence of the window concept.
    ssc.checkpoint(checkPointDir)

    // Get the list of topic used by kafka
    val topicArr = topics.split(",")
    val topicSet = topicArr.toSet
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "DemoConsumer"
    );

    val locationStrategy = LocationStrategies.PreferConsistent
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)

    // Create direct kafka stream with brokers and topics
    // Receive data from the Kafka and generate the corresponding DStream
    val stream = KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, consumerStrategy)

    // Obtain field properties in each row.
    val tf = stream.transform ( rdd =>
      rdd.map(r => (r.value, 1L))
    )

    // Aggregate the total time that calculate word count
    val wordCounts = tf.reduceByKey(_ + _)
    val totalCounts = wordCounts.updateStateByKey(updataFunc)
    totalCounts.print()

    ssc
  }

  def updataFunc(values : Seq[Long], state : Option[Long]) : Option[Long] =
    Some(values.sum + state.getOrElse(0L))
}
