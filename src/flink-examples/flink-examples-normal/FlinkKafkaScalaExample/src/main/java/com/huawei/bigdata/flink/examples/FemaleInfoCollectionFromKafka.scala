package com.huawei.bigdata.flink.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object FemaleInfoCollectionFromKafka {
  def main(args: Array[String]) {
    // print comment for command to use run flink
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.FemaleInfoCollectionFromKafka" +
      " /opt/test.jar --windowTime 2 --topic topic-test --bootstrap.servers xxx.xxx.xxx.xxx:21005")
    System.out.println("******************************************************************************************")
    System.out.println("<windowTime> is the width of the window, time as minutes")
    System.out.println("<topic> is the kafka topic name")
    System.out.println("<bootstrap.servers> is the ip:port list of brokers")
    System.out.println("******************************************************************************************")

    // prepare for the DataStream build
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val paraTool = ParameterTool.fromArgs(args)
    val windowTime = paraTool.getInt("windowTime", 2)
    val messageStream = env.addSource(new FlinkKafkaConsumer[String](
      paraTool.get("topic"), new SimpleStringSchema, paraTool.getProperties))

    // get input data
    messageStream.map(getRecord(_))
      .assignTimestampsAndWatermarks(new Record2TimestampExtractor)
      .filter(_.gender == "female")
      .keyBy("name", "gender")
      .window(TumblingEventTimeWindows.of(Time.minutes(windowTime)))
      .reduce((e1, e2) => UserRecord(e1.name, e1.gender, e1.shoppingTime + e2.shoppingTime))
      .filter(_.shoppingTime > 120).print()

    // go to execute
    env.execute("FemaleInfoCollectionPrint scala")
  }

  // get enums of record
  def getRecord(line: String): UserRecord = {
    val elems = line.split(",")
    assert(elems.length == 3)
    val name = elems(0)
    val gender = elems(1)
    val time = elems(2).toInt
    UserRecord(name, gender, time)
  }

  // the scheme of record read from txt
  case class UserRecord(name: String, gender: String, shoppingTime: Int)

  // class to set watermark and timestamp
  private class Record2TimestampExtractor extends AssignerWithPunctuatedWatermarks[UserRecord] {

    // add tag in the data of datastream elements
    override def extractTimestamp(element: UserRecord, previousTimestamp: Long): Long = {
      System.currentTimeMillis()
    }

    // give the watermark to trigger the window to execute, and use the value to check if the window elements is ready
    def checkAndGetNextWatermark(lastElement: UserRecord,
                                 extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - 1)
    }
  }
}