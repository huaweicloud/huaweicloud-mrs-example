package com.huawei.bigdata.flink.examples

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class UserSource extends RichParallelSourceFunction[Inforamtion] with Serializable{

  var isRunning = true

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val parallelism = getRuntimeContext.getNumberOfParallelSubtasks
    println("The parallelism is: " + parallelism)
  }

  override def run(sourceContext: SourceContext[Inforamtion]) = {

    while (isRunning) {
      for (i <- 0 until 10000) {
        sourceContext.collect(Inforamtion(i, "hello-" + i));

      }
      Thread.sleep(1000)
    }
  }

  override def close(): Unit = super.close()

  override def cancel() = {
    isRunning = false
  }
}
