package com.huawei.bigdata.spark.examples

import java.io.{BufferedReader, InputStream, InputStreamReader}

import org.apache.spark.launcher.SparkLauncher

/**
  * Submit spark app.
  * args(0) is the mode to run spark app, eg yarn-client
  * args(1) is the path of spark app jar
  * args(2) is the main class of spark app
  * args(3...) is the parameters of spark app
  */
object SparkLauncherExample {
  def main(args: Array[String]) {
    println(s"com.huawei.bigdata.spark.examples.SparkLauncherExample <mode> <jarParh>  <app_main_class> <appArgs>")
    val launcher = new SparkLauncher()
    launcher.setMaster(args(0))
      .setAppResource(args(1)) // Specify user app jar path
      .setMainClass(args(2))
      if (args.drop(3).length > 0) {
        // Set app args
        launcher.addAppArgs(args.drop(3): _*)
      }


    // Launch the app
    val process = launcher.launch()
    // Get Spark driver log
    new Thread(new ISRRunnable(process.getErrorStream)).start()
    val exitCode = process.waitFor()
    println(s"Finished! Exit code is $exitCode")
  }
}

/**
  * Print the log of process
  */
class ISRRunnable(reader: BufferedReader) extends Runnable {
  def this(inputStream: InputStream) {
    this(new BufferedReader(new InputStreamReader(inputStream)))
  }

  @Override def run(): Unit = {
    var line = reader.readLine()
    while (line != null) {
      println(line)
      line = reader.readLine()
    }
    reader.close()
  }
}