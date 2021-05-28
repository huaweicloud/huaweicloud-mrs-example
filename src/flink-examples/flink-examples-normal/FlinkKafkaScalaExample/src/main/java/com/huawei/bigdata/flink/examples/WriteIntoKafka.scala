package com.huawei.bigdata.flink.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._

object WriteIntoKafka {
  def main(args: Array[String]) {
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka" +
      " /opt/test.jar --topic topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21005")
    System.out.println("******************************************************************************************")
    System.out.println("<topic> is the kafka topic name")
    System.out.println("<bootstrap.servers> is the ip:port list of brokers")
    System.out.println("******************************************************************************************")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val paraTool = ParameterTool.fromArgs(args)
    val messageStream: DataStream[String] = env.addSource(new SimpleStringGenerator)
    messageStream.addSink(new FlinkKafkaProducer(
      paraTool.get("topic"), new SimpleStringSchema, paraTool.getProperties))
    env.execute
  }
}

class SimpleStringGenerator extends SourceFunction[String] {
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