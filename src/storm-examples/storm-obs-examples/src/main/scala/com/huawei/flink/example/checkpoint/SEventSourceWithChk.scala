package com.huawei.flink.example.checkpoint

import java.util
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

case class SEvent(id: Long, name: String, info: String, count: Int)

// è¯¥ç±»æ¯å¸¦æcheckpointçsourceç®å­
class SEventSourceWithChk extends RichSourceFunction[SEvent] with ListCheckpointed[UDFStateScala]{
  private var count = 0L
  private var isRunning = true
  private val alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWZYX0987654321"

  // sourceç®å­çé»è¾ï¼å³ï¼æ¯ç§éåæµå¾ä¸­æ³¨å¥10000ä¸ªåç»
  override def run(sourceContext: SourceContext[SEvent]): Unit = {
    while(isRunning) {
      for (i <- 0 until 10000) {
        sourceContext.collect(SEvent(1, "hello-"+count, alphabet,1))
        count += 1L
      }
      Thread.sleep(1000)
    }
  }

  // ä»»å¡åæ¶æ¶è°ç¨
  override def cancel(): Unit = {
    isRunning = false;
  }

  override def close(): Unit = super.close()

  // å¶ä½å¿«ç§
  override def snapshotState(l: Long, l1: Long): util.List[UDFStateScala] = {
    val udfList: util.ArrayList[UDFStateScala] = new util.ArrayList[UDFStateScala]
    val udfState = new UDFStateScala
    udfState.setState(count)
    udfList.add(udfState)
    udfList
  }

  // ä»å¿«ç§ä¸­è·åç¶æ
  override def restoreState(list: util.List[UDFStateScala]): Unit = {
    val udfState = list.get(0)
    count = udfState.getState
  }
}
