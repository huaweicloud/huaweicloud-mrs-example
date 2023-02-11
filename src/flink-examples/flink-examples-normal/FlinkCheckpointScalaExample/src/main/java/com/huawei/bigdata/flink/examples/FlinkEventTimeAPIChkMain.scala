package com.huawei.bigdata.flink.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object FlinkEventTimeAPIChkMain {

  def main(args: Array[String]): Unit ={

    println("use command as: ")
    println("bin/flink run --class com.huawei.bigdata.flink.examples.FlinkEventTimeAPIChkMain " +
      "<path of FlinkCheckpointScalaExample jar>  --chkPath <checkpoint path>")
    println("*********************************************************************************")
    println("checkpoint path should be start with hdfs:// or file://")
    println("*********************************************************************************")

    val paraTool = ParameterTool.fromArgs(args)
    val chkPath = paraTool.get("chkPath")
    if (chkPath == null) {
      println("NO checkpoint path is given!")
      System.exit(1)
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend(chkPath))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(2000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(6000)

    env.addSource(new SEventSourceWithChk)
    .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[SEvent] {
      override def getCurrentWatermark: Watermark = {
        new Watermark(System.currentTimeMillis())
      }

      override def extractTimestamp(t: SEvent, l: Long): Long = {
        System.currentTimeMillis()
      }
    })
    .keyBy(0)
    .window(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(1)))
    .apply(new WindowStatisticWithChk)
    .print()

    env.execute()
  }
}
