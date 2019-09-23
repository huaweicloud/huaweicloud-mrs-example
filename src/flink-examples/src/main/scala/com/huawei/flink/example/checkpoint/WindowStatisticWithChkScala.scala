package com.huawei.flink.example.checkpoint

import java.util
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// è¯¥ç±»æ¯å¸¦checkpointçwindowç®å­
class WindowStatisticWithChkScala extends WindowFunction[SEvent, Long, Tuple, TimeWindow] with ListCheckpointed[UDFStateScala]{
  private var total = 0L

  // windowç®å­çå®ç°é»è¾ï¼å³ï¼ç»è®¡windowä¸­åç»çæ°é
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SEvent], out: Collector[Long]): Unit = {
    var count = 0L
    for (event <- input) {
      count += 1L
    }
    total += count
    out.collect(count)
  }

  // å¶ä½èªå®ä¹ç¶æå¿«ç§
  override def snapshotState(l: Long, l1: Long): util.List[UDFStateScala] = {
    val udfList: util.ArrayList[UDFStateScala] = new util.ArrayList[UDFStateScala]
    val udfState = new UDFStateScala
    udfState.setState(total)
    udfList.add(udfState)
    udfList
  }

  // ä»èªå®ä¹å¿«ç§ä¸­æ¢å¤ç¶æ
  override def restoreState(list: util.List[UDFStateScala]): Unit = {
    val udfState = list.get(0)
    total = udfState.getState
  }
}
