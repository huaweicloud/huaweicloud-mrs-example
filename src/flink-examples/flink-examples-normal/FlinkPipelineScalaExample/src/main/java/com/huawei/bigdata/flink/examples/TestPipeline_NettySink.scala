package com.huawei.bigdata.flink.examples

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.netty.sink.NettySink
import org.apache.flink.streaming.connectors.netty.utils.ZookeeperRegisterServerHandler
import org.apache.flink.streaming.api.scala._

object TestPipeline_NettySink {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val zkRegisterServerHandler = new ZookeeperRegisterServerHandler
    env.addSource(new UserSource)
      .keyBy(0).map(x=>x.content.getBytes)
      .addSink(new NettySink("NettySink-1", "TOPIC-2", zkRegisterServerHandler, 2))

    env.execute()
  }
}
