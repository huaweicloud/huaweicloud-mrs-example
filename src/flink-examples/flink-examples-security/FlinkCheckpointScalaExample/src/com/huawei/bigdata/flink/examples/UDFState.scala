package com.huawei.bigdata.flink.examples

class UDFState extends Serializable{

  private var count = 0L
  def setState(s: Long) = count = s
  def getState = count
}
