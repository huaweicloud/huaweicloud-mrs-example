package com.huawei.bigdata.spark.examples


@SerialVersionUID(0)
case class SolrException(message: String, cause: Throwable) extends Exception(message, cause)
