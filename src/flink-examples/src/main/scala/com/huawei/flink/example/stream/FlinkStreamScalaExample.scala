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
    // æå°åºæ§è¡flink runçåèå½ä»¤
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class com.huawei.flink.example.stream.FlinkStreamScalaExample /opt/test.jar --filePath /opt/log1.txt,/opt/log2.txt --windowTime 2")
    System.out.println("******************************************************************************************")
    System.out.println("<filePath> is for text file to read data, use comma to separate")
    System.out.println("<windowTime> is the width of the window, time as minutes")
    System.out.println("******************************************************************************************")

    // è¯»åææ¬è·¯å¾ä¿¡æ¯ï¼å¹¶ä½¿ç¨éå·åé
    val filePaths = ParameterTool.fromArgs(args).get("filePath", "/opt/log1.txt,/opt/log2.txt").split(",").map(_.trim)
    assert(filePaths.length > 0)

    // windowTimeè®¾ç½®çªå£æ¶é´å¤§å°ï¼é»è®¤2åéä¸ä¸ªçªå£è¶³å¤è¯»åææ¬åçæææ°æ®äº
    val windowTime = ParameterTool.fromArgs(args).getInt("windowTime", 2)

    // æé æ§è¡ç¯å¢ï¼ä½¿ç¨eventTimeå¤ççªå£æ°æ®
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // è¯»åææ¬æ°æ®æµ
    val unionStream = if (filePaths.length > 1) {
      val firstStream = env.readTextFile(filePaths.apply(0))
      firstStream.union(filePaths.drop(1).map(it => env.readTextFile(it)): _*)
    } else {
      env.readTextFile(filePaths.apply(0))
    }

    // æ°æ®è½¬æ¢ï¼æé æ´ä¸ªæ°æ®å¤ççé»è¾ï¼è®¡ç®å¹¶å¾åºç»ææå°åºæ¥
    unionStream.map(getRecord(_))
      .assignTimestampsAndWatermarks(new Record2TimestampExtractor)
      .filter(_.sexy == "female")
      .keyBy("name", "sexy")
      .window(TumblingEventTimeWindows.of(Time.minutes(windowTime)))
      .reduce((e1, e2) => UserRecord(e1.name, e1.sexy, e1.shoppingTime + e2.shoppingTime))
      .filter(_.shoppingTime > 120).print()

    // è°ç¨executeè§¦åæ§è¡
    env.execute("FemaleInfoCollectionPrint scala")
  }

  // è§£æææ¬è¡æ°æ®ï¼æé UserRecordæ°æ®ç»æ
  def getRecord(line: String): UserRecord = {
    val elems = line.split(",")
    assert(elems.length == 3)
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    UserRecord(name, sexy, time)
  }

  // UserRecordæ°æ®ç»æçå®ä¹
  case class UserRecord(name: String, sexy: String, shoppingTime: Int)


  // æé ç»§æ¿AssignerWithPunctuatedWatermarksçç±»ï¼ç¨äºè®¾ç½®eventTimeä»¥åwaterMark
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
