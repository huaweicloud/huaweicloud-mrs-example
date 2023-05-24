package com.huawei.bigdata.flink.examples

import java.util
import java.util.{ArrayList, List}

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

class SEventSourceWithChk extends RichSourceFunction[SEvent] with ListCheckpointed[UDFState]{

  private var count = 0L
  private var isRunning = true

  private[examples] val listState = new util.ArrayList[UDFState]

  private val alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWZYX0987654321"

  override def run(sourceContext: SourceFunction.SourceContext[SEvent]): Unit = {
    while(isRunning) {
      for (i <- 0 until 10000) {
        sourceContext.collect(SEvent(Random.nextLong(), "hello-" + count, alphabet, 1))
        count += 1L
      }
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  override def snapshotState(l: Long, l1: Long): util.List[UDFState] = {
    val udfList: util.ArrayList[UDFState] = new util.ArrayList[UDFState]()
    val udfState = new UDFState
    udfState.setState(count)
    udfList.add(udfState)
    udfList
  }

  override def restoreState(list: util.List[UDFState]): Unit = {
    if (listState.size > 0) {
      val udfState = list.get(0)
      count = udfState.getState
    }
  }

}
