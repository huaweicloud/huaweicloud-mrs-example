package com.huawei.bigdata.flink.examples

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import java.util.Random

object WriteIntoKafka {
  def main(args: Array[String]): Unit = {
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka" + " /opt/test.jar --topic topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21005")
    System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka /opt/test.jar --topic" + " topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21007 --security.protocol SASL_PLAINTEXT" + " --sasl.kerberos.service.name kafka")
    System.out.println("******************************************************************************************")
    System.out.println("<topic> is the kafka topic name")
    System.out.println("<bootstrap.servers> is the ip:port list of brokers")
    System.out.println("******************************************************************************************")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val paraTool = ParameterTool.fromArgs(args)

    val messageStream = env.addSource(new WriteIntoKafka.SimpleStringGenerator)
    val producer = new FlinkKafkaProducer[String](paraTool.get("topic"), new SimpleStringSchema, paraTool.getProperties)

    producer.setWriteTimestampToKafka(true)

    messageStream.addSink(producer)
    env.execute
  }

  /**
   * String source类
   *
   */
  object SimpleStringGenerator {
    private[examples] val NAME = Array("Carry", "Alen", "Mike", "Ian", "John", "Kobe", "James")
    private[examples] val GENDER = Array("MALE", "FEMALE")
    private[examples] val COUNT = NAME.length
  }

  class SimpleStringGenerator extends SourceFunction[String] {
    private[examples] var running = true
    private[examples] val rand = new Random(47)

    @throws[Exception]
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      while (running) {
        val i = rand.nextInt(SimpleStringGenerator.COUNT)
        val age = rand.nextInt(70)
        val gender = SimpleStringGenerator.GENDER(rand.nextInt(2))
        ctx.collect(SimpleStringGenerator.NAME(i) + "," + age + "," + gender)
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

}
