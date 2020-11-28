package com.huawei.bigdata.spark.examples

import java.util.concurrent.TimeUnit
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat_ws, expr}

case class ReqEvent(reqAdID: String, reqTime: Timestamp)
case class ShowEvent(showAdID: String, showID: String, showTime: Timestamp)
case class ClickEvent(clickAdID: String, clickShowID: String, clickTime: Timestamp)

object KafkaADCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      System.err.println("Usage: KafkaWordCount <bootstrap-servers> " +
        "<maxEventDelay> <reqTopic> <showTopic> <maxShowDelay> " +
        "<clickTopic> <maxClickDelay> <triggerInterver> " +
        "<checkpointLocation>")
      System.exit(1)
    }

    val Array(bootstrapServers, maxEventDelay, reqTopic, showTopic,
    maxShowDelay, clickTopic, maxClickDelay, triggerInterver, checkpointLocation) = args

    val maxEventDelayMills = JavaUtils.timeStringAs(maxEventDelay, TimeUnit.MILLISECONDS)
    val maxShowDelayMills = JavaUtils.timeStringAs(maxShowDelay, TimeUnit.MILLISECONDS)
    val maxClickDelayMills = JavaUtils.timeStringAs(maxClickDelay, TimeUnit.MILLISECONDS)
    val triggerMills = JavaUtils.timeStringAs(triggerInterver, TimeUnit.MILLISECONDS)

    val spark = SparkSession
      .builder
      .appName("KafkaADCount")
      .getOrCreate()

    spark.conf.set("spark.sql.streaming.checkpointLocation", checkpointLocation)

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val reqDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", reqTopic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map{
        _.split('^') match {
          case Array(reqAdID, reqTime) => ReqEvent(reqAdID,
            Timestamp.valueOf(reqTime))
        }
      }
      .as[ReqEvent]
      .withWatermark("reqTime", maxEventDelayMills +
        maxShowDelayMills + " millisecond")

    val showDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", showTopic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map{
        _.split('^') match {
          case Array(showAdID, showID, showTime) => ShowEvent(showAdID,
            showID, Timestamp.valueOf(showTime))
        }
      }
      .as[ShowEvent]
      .withWatermark("showTime", maxEventDelayMills +
        maxShowDelayMills + maxClickDelayMills + " millisecond")

    val clickDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", clickTopic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map{
        _.split('^') match {
          case Array(clickAdID, clickShowID, clickTime) => ClickEvent(clickAdID,
            clickShowID, Timestamp.valueOf(clickTime))
        }
      }
      .as[ClickEvent]
      .withWatermark("clickTime", maxEventDelayMills + " millisecond")

    val showStaticsQuery = reqDf.join(showDf,
      expr(s"""
      reqAdID = showAdID
      AND showTime >= reqTime + interval ${maxShowDelayMills} millisecond
      """))
      .selectExpr("concat_ws('^', showAdID, showID, showTime) as value")
      .writeStream
      .queryName("showEventStatics")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(triggerMills.millis))
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", "showEventStatics")
      .start()

    val clickStaticsQuery = showDf.join(clickDf,
      expr(s"""
      showAdID = clickAdID AND
      showID = clickShowID AND
      clickTime >= showTime + interval ${maxClickDelayMills} millisecond
      """), joinType = "rightouter")
      .dropDuplicates("showAdID")
      .selectExpr("concat_ws('^', clickAdID, clickShowID, clickTime) as value")
      .writeStream
      .queryName("clickEventStatics")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(triggerMills.millis))
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", "clickEventStatics")
      .start()

    new Thread(new Runnable {
      override def run(): Unit = {
        while(true) {
          println("-------------get showStatic progress---------")
          //println(showStaticsQuery.lastProgress)
          println(showStaticsQuery.status)
          println("-------------get clickStatic progress---------")
          //println(clickStaticsQuery.lastProgress)
          println(clickStaticsQuery.status)
          Thread.sleep(10000)
        }
      }
    }).start

    spark.streams.awaitAnyTermination()

  }
}