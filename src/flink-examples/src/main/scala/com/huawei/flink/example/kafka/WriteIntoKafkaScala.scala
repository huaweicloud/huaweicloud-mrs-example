package com.huawei.flink.example.kafka

import com.huawei.flink.example.common.SimpleStringGeneratorScala
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.api.scala._

object WriteIntoKafkaScala {
  def main(args: Array[String]) {
    // 打印出执行flink run的参考命令
    System.out.println("use command as: ")

    System.out.println("./bin/flink run --class com.huawei.flink.example.kafka.WriteIntoKafkaScala" +

      " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21005")

    System.out.println
    ("******************************************************************************************")

    System.out.println("<topic> is the kafka topic name")

    System.out.println("<bootstrap.servers> is the ip:port list of brokers")

    System.out.println
    ("******************************************************************************************")
    // 构造执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并发度
    env.setParallelism(1)
    // 解析运行参数
    val paraTool = ParameterTool.fromArgs(args)
    // 构造流图，将自定义Source生成的数据写入Kafka
    val messageStream: DataStream[String] = env.addSource(new SimpleStringGeneratorScala)

    messageStream.addSink(new FlinkKafkaProducer(paraTool.get("topic"), new SimpleStringSchema, paraTool.getProperties))
    // 调用execute触发执行
    env.execute

  }

}
