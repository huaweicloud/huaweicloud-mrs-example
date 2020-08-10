package com.huawei.flink.example.checkpoint

import java.util
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 该类是带checkpoint的window算子
class WindowStatisticWithChkScala extends WindowFunction[SEvent, Long, Tuple, TimeWindow] with ListCheckpointed[UDFStateScala]{
  private var total = 0L

  // window算子的实现逻辑，即：统计window中元组的数量
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SEvent], out: Collector[Long]): Unit = {
    var count = 0L
    for (event <- input) {
      count += 1L
    }
    total += count
    out.collect(count)
  }

  // 制作自定义状态快照
  override def snapshotState(l: Long, l1: Long): util.List[UDFStateScala] = {
    val udfList: util.ArrayList[UDFStateScala] = new util.ArrayList[UDFStateScala]
    val udfState = new UDFStateScala
    udfState.setState(total)
    udfList.add(udfState)
    udfList
  }

  // 从自定义快照中恢复状态
  override def restoreState(list: util.List[UDFStateScala]): Unit = {
    val udfState = list.get(0)
    total = udfState.getState
  }
}
