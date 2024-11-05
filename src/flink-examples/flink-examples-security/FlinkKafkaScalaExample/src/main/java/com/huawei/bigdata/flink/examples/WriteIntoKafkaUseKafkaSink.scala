package com.huawei.bigdata.flink.examples

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object WriteIntoKafkaUseKafkaSink {
  def main(args: Array[String]): Unit = {
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class class com.huawei.bigdata.flink.examples.WriteIntoKafkaUseKafkaSink" +
      " /opt/test.jar --topic topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21005")
    System.out.println(
      "./bin/flink run --class class com.huawei.bigdata.flink.examples.WriteIntoKafkaUseKafkaSink /opt/test.jar --topic"
        + " topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21007 --security.protocol SASL_PLAINTEXT"
        + " --sasl.kerberos.service.name kafka --kerberos.domain.name hadoop.系统域名");
    System.out.println("******************************************************************************************")
    System.out.println("<topic> is the kafka topic name")
    System.out.println("<bootstrap.servers> is the ip:port list of brokers")
    System.out.println("******************************************************************************************")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val paraTool = ParameterTool.fromArgs(args)
    val messageStream: DataStream[String] = env.addSource(new SimpleStringGenerator2)
    val build = KafkaSink.builder[String]
      .setBootstrapServers(paraTool.get("bootstrap.servers"))
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]
        .setValueSerializationSchema(new SimpleStringSchema())
        .setTopic(paraTool.get("topic"))
        .build)
      .setKafkaProducerConfig(paraTool.getProperties)
      .build
    messageStream.sinkTo(build)
    env.execute
  }
}

class SimpleStringGenerator2 extends SourceFunction[String] {
  var running = true
  var i = 0

  override def run(ctx: SourceContext[String]) {
    while (running) {
      ctx.collect("element-" + i)
      i += 1
      Thread.sleep(1000)
    }
  }

  override def cancel() {
    running = false
  }
}