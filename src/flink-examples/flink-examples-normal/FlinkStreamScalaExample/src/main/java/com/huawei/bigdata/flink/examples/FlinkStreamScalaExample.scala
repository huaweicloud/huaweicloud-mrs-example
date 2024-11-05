package com.huawei.bigdata.flink.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Consumes messages from one or more topics in Kafka.
 * <filePath> is for text file to read data, use comma to separate
 * <windowTime> is the width of the window, time as minutes
 */

object FlinkStreamScalaExample {
  def main(args: Array[String]) {
    // print comment for command to use run flink
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.FlinkStreamScalaExample"
      + " /opt/test.jar --filePath /opt/log1.txt,/opt/log2.txt --windowTime 2")
    System.out.println("******************************************************************************************")
    System.out.println("<filePath> is for text file to read data, use comma to separate")
    System.out.println("<windowTime> is the width of the window, time as minutes")
    System.out.println("******************************************************************************************")

    // filePaths to read data from, split with comma
    val filePaths = ParameterTool.fromArgs(args).get("filePath",
      "/opt/log1.txt,/opt/log2.txt").split(",").map(_.trim)
    assert(filePaths.length > 0)

    // windowTime is for the width of window, as the data read from filePaths quickly
    // , 2 minutes is enough to read all data
    val windowTime = ParameterTool.fromArgs(args).getInt("windowTime", 2)

    // prepare for the DataStream build
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // support filePaths split with comma, use union function to union DataStreams
    val unionStream = if (filePaths.length > 1) {
      val firstStream = env.readTextFile(filePaths.apply(0))
      firstStream.union(filePaths.drop(1).map(it => env.readTextFile(it)): _*)
    } else {
      env.readTextFile(filePaths.apply(0))
    }

    // get input data
    unionStream.map(getRecord(_))
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