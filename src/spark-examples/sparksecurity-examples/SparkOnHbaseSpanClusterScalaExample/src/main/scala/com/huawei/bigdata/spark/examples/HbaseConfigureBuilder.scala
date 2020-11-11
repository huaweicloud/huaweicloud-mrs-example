package com.huawei.bigdata.spark.examples

class HbaseConfigureBuilder {
  var columnfamily: String = ""
  var defKey: String = ""
  var tableName: String = ""
  var saveHfilePath: String = ""
  var sourceHdfsPath: String = ""
  def setCloumnfamily(columnfamily: String) : HbaseConfigureBuilder = {
    this.columnfamily = columnfamily
    this
  }
  def setDefKey(defKey: String) : HbaseConfigureBuilder = {
    this.defKey = defKey
    this
  }
  def setTableName(tableName: String) : HbaseConfigureBuilder = {
    this.tableName = tableName
    this
  }
  def setSaveHfilePath(saveHfilePath: String) : HbaseConfigureBuilder = {
    this.saveHfilePath = saveHfilePath
    this
  }
  def setSourceHdfsPath(sourceHdfsPath: String) : HbaseConfigureBuilder = {
    this.sourceHdfsPath = sourceHdfsPath
    this
  }
  def build() = {
    new HbaseConfigure(this)
  }
}
