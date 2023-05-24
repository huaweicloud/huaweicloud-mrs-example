package com.huawei.bigdata.flink.examples

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

object ReadFromKafka {
  def main(args: Array[String]) {
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.ReadFromKafka" +
      " /opt/test.jar --topic topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21005")
    System.out.println("******************************************************************************************")
    System.out.println("<topic> is the kafka topic name")
    System.out.println("<bootstrap.servers> is the ip:port list of brokers")
    System.out.println("******************************************************************************************")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val paraTool = ParameterTool.fromArgs(args)
    val messageStream = env.addSource(new FlinkKafkaConsumer(
      paraTool.get("topic"), new SimpleStringSchema, paraTool.getProperties))
    messageStream
      .map(s => "Flink says " + s + System.getProperty("line.separator")).print()
    env.execute()
  }
}