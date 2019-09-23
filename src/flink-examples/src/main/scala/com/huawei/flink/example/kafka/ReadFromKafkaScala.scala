package com.huawei.flink.example.kafka

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._

object ReadFromKafkaScala {
  def main(args: Array[String]) {
    // æå°åºæ§è¡flink runçåèå½ä»¤
    System.out.println("use command as: ")

    System.out.println("./bin/flink run --class com.huawei.flink.example.kafka.ReadFromKafkaScala" +

      " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21005")

    System.out.println
    ("******************************************************************************************")

    System.out.println("<topic> is the kafka topic name")

    System.out.println("<bootstrap.servers> is the ip:port list of brokers")

    System.out.println
    ("******************************************************************************************")


    // æé æ§è¡ç¯å¢
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // è®¾ç½®å¹¶ååº¦
    env.setParallelism(1)
    // è§£æè¿è¡åæ°
    val paraTool = ParameterTool.fromArgs(args)
    // æé æµå¾ï¼ä»Kafkaè¯»åæ°æ®å¹¶æ¢è¡æå°
    val messageStream = env.addSource(new FlinkKafkaConsumer(

      paraTool.get("topic"), new SimpleStringSchema, paraTool.getProperties))

    messageStream

      .map(s => "Flink says " + s + System.getProperty("line.separator")).print()
    // è°ç¨executeè§¦åæ§è¡
    env.execute()

  }

}
