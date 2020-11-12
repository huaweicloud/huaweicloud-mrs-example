package com.huawei.bigdata.spark.examples

class HbaseConfigure(hbaseConfigureBuilder: HbaseConfigureBuilder)
{
  var columnfamily: String = hbaseConfigureBuilder.columnfamily
  var defKey: String = hbaseConfigureBuilder.defKey
  var tableName: String = hbaseConfigureBuilder.tableName
  var savePath: String = hbaseConfigureBuilder.saveHfilePath
  var sourceHdfsPath: String = hbaseConfigureBuilder.sourceHdfsPath
}
