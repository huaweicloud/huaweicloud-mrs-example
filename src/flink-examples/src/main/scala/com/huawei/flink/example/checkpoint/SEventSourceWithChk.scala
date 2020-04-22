package com.huawei.flink.example.checkpoint

import java.util
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

case class SEvent(id: Long, name: String, info: String, count: Int)

// 该类是带有checkpoint的source算子
class SEventSourceWithChk extends RichSourceFunction[SEvent] with ListCheckpointed[UDFStateScala]{
  private var count = 0L
  private var isRunning = true
  private val alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWZYX0987654321"

  // source算子的逻辑，即：每秒钟向流图中注入10000个元组
  override def run(sourceContext: SourceContext[SEvent]): Unit = {
    while(isRunning) {
      for (i <- 0 until 10000) {
        sourceContext.collect(SEvent(1, "hello-"+count, alphabet,1))
        count += 1L
      }
      Thread.sleep(1000)
    }
  }

  // 任务取消时调用
  override def cancel(): Unit = {
    isRunning = false;
  }

  override def close(): Unit = super.close()

  // 制作快照
  override def snapshotState(l: Long, l1: Long): util.List[UDFStateScala] = {
    val udfList: util.ArrayList[UDFStateScala] = new util.ArrayList[UDFStateScala]
    val udfState = new UDFStateScala
    udfState.setState(count)
    udfList.add(udfState)
    udfList
  }

  // 从快照中获取状态
  override def restoreState(list: util.List[UDFStateScala]): Unit = {
    val udfState = list.get(0)
    count = udfState.getState
  }
}
