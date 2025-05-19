package com.huawei.bigdata.spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FemaleInfoCollection {
  def main (args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: CollectFemaleInfo <file>")
      System.exit(1)
    }

    // Configure the Spark application name.
    val spark = SparkSession
      .builder()
      .appName("CollectFemaleInfo")
      .getOrCreate()
    // Initializing Spark

    // Read data. This code indicates the data path that the input parameter args(0) specifies.
    val text = spark.sparkContext.textFile(args(0))
    // Filter the data information about the time that female netizens spend online.
    val data = text.filter(_.contains("female"))

    // Aggregate the time that each female netizen spends online
    val femaleData: RDD[(String, Int)] = data.map { line =>
      val t = line.split(',')
      (t(0), t(2).toInt)
    }.reduceByKey(_ + _)

    // Filter the information about female netizens who spend more than 2 hours online, and export the results
    val result = femaleData.filter(line => line._2 > 120)
    result.collect().map(x => x._1 + ',' + x._2).foreach(println)

    spark.stop()
  }
}
