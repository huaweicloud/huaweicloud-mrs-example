package com.huawei.flink.example.checkpoint

class UDFStateScala extends Serializable{
  private var count = 0L

  // 设置用户自定义状态
  def setState(s: Long) = count = s

  // 获取用户自定状态
  def getState = count
}
