package com.huawei.flink.example.checkpoint

class UDFStateScala extends Serializable{
  private var count = 0L

  // è®¾ç½®ç¨æ·èªå®ä¹ç¶æ
  def setState(s: Long) = count = s

  // è·åç¨æ·èªå®ç¶æ
  def getState = count
}
