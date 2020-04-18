package com.huawei.flink.example.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

object FlinkStreamScalaExample {
  def main(args: Array[String]) {
    // 打印出执行flink run的参考命令
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class com.huawei.flink.example.stream.FlinkStreamScalaExample /opt/test.jar --filePath /opt/log1.txt,/opt/log2.txt --windowTime 2")
    System.out.println("******************************************************************************************")
    System.out.println("<filePath> is for text file to read data, use comma to separate")
    System.out.println("<windowTime> is the width of the window, time as minutes")
    System.out.println("******************************************************************************************")

    // 读取文本路径信息，并使用逗号分隔
    val filePaths = ParameterTool.fromArgs(args).get("filePath", "/opt/log1.txt,/opt/log2.txt").split(",").map(_.trim)
    assert(filePaths.length > 0)

    // windowTime设置窗口时间大小，默认2分钟一个窗口足够读取文本内的所有数据了
    val windowTime = ParameterTool.fromArgs(args).getInt("windowTime", 2)

    // 构造执行环境，使用eventTime处理窗口数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取文本数据流
    val unionStream = if (filePaths.length > 1) {
      val firstStream = env.readTextFile(filePaths.apply(0))
      firstStream.union(filePaths.drop(1).map(it => env.readTextFile(it)): _*)
    } else {
      env.readTextFile(filePaths.apply(0))
    }

    // 数据转换，构造整个数据处理的逻辑，计算并得出结果打印出来
    unionStream.map(getRecord(_))
      .assignTimestampsAndWatermarks(new Record2TimestampExtractor)
      .filter(_.sexy == "female")
      .keyBy("name", "sexy")
      .window(TumblingEventTimeWindows.of(Time.minutes(windowTime)))
      .reduce((e1, e2) => UserRecord(e1.name, e1.sexy, e1.shoppingTime + e2.shoppingTime))
      .filter(_.shoppingTime > 120).print()

    // 调用execute触发执行
    env.execute("FemaleInfoCollectionPrint scala")
  }

  // 解析文本行数据，构造UserRecord数据结构
  def getRecord(line: String): UserRecord = {
    val elems = line.split(",")
    assert(elems.length == 3)
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    UserRecord(name, sexy, time)
  }

  // UserRecord数据结构的定义
  case class UserRecord(name: String, sexy: String, shoppingTime: Int)


  // 构造继承AssignerWithPunctuatedWatermarks的类，用于设置eventTime以及waterMark
  private class Record2TimestampExtractor extends AssignerWithPunctuatedWatermarks[UserRecord] {

    // add tag in the data of datastream elements
    override def extractTimestamp(element: UserRecord, previousTimestamp: Long): Long = {
      System.currentTimeMillis()
    }

    // give the watermark to trigger the window to execute, and use the value to check if the window elements is ready
    def checkAndGetNextWatermark(lastElement: UserRecord, extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - 1)
    }
  }

}
