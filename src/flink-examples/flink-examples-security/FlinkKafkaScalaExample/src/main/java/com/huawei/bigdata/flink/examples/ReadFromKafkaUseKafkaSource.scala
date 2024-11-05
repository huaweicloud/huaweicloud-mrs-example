package com.huawei.bigdata.flink.examples

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object ReadFromKafkaUseKafkaSource {
  def main(args: Array[String]): Unit = {
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class class com.huawei.bigdata.flink.examples.ReadFromKafkaUseKafkaSource" +
      " /opt/test.jar --topic topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21005")
    System.out.println(
      "./bin/flink run --class com.huawei.bigdata.flink.examples.ReadFromKafkaUseKafkaSource /opt/test.jar --topic"
        + " topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21007 --security.protocol SASL_PLAINTEXT"
        + " --sasl.kerberos.service.name kafka --kerberos.domain.name hadoop.系统域名");
    System.out.println("******************************************************************************************")
    System.out.println("<topic> is the kafka topic name")
    System.out.println("<bootstrap.servers> is the ip:port list of brokers")
    System.out.println("******************************************************************************************")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val paraTool = ParameterTool.fromArgs(args)
    val messageStream = env.fromSource(KafkaSource.builder[String]
      .setBootstrapServers(paraTool.get("bootstrap.servers"))
      .setTopics(paraTool.get("topic"))
      .setStartingOffsets(OffsetsInitializer.earliest)
      .setProperties(paraTool.getProperties)
      .build(), WatermarkStrategy.noWatermarks(), "Kafka Source")
    messageStream
      .map(s => "Flink says " + s + System.getProperty("line.separator")).print()
    env.execute()
  }
}
