package com.huawei.spark.examples

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.SparkContext


class HbaseUtil(sc: SparkContext, conf: Configuration) {

  private val connection = ConnectionFactory.createConnection(conf)

  /**
    * create table
    *
    * @param tableName
    */
  private def createTable(tableName: String): Boolean = {
    val columnFamily = "INFO"
    var admin: Admin = null
    var isTableExised: Boolean = false

    try {
      admin = connection.getAdmin
      if (admin.tableExists(TableName.valueOf(tableName))) {
        println(s"table $tableName existed")
        isTableExised = true
      } else {
        val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
        val columnDesc = new HColumnDescriptor(columnFamily.getBytes)
        tableDesc.addFamily(columnDesc)
        admin.createTable(tableDesc)
        println(s"$tableName create success!")
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    isTableExised
  }


  /**
    * writer to table
    */
  def writeToHbase(tableName: String): Unit = {
    createTable(tableName)
    val familyName = Bytes.toBytes("INFO")
    var table: Table = null
    try {
      table = connection.getTable(TableName.valueOf(tableName))

      val data = Array("a", "b", "c", "d")
      var i = 0
      for (it <- data) {
        val put = new Put(Bytes.toBytes("row" + i));
        put.addColumn(familyName, Bytes.toBytes("c1"), Bytes.toBytes(it))
        put.addColumn(familyName, Bytes.toBytes("c2"), Bytes.toBytes(it))
        put.addColumn(familyName, Bytes.toBytes("c3"), Bytes.toBytes(it))
        put.addColumn(familyName, Bytes.toBytes("c4"), Bytes.toBytes(it))
        i += 1
        table.put(put)
      }

    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      try {
        connection.close()
      } catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
  }



  def readFromHbase(tableName: String): Unit = {
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("INFO"))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = TableMapReduceUtil.convertScanToString(new Scan())
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
//    conf.set(TableInputFormat.SCAN, scanToString)

    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    //遍历hbase表中的每一行，并打印结果
    rdd.collect().foreach(x => {
      val key = x._1.toString
      val it = x._2.listCells().iterator()
      while (it.hasNext) {
        val c = it.next()
        val family = Bytes.toString(CellUtil.cloneFamily(c))
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(c))
        val value = Bytes.toString(CellUtil.cloneValue(c))
        val tm = c.getTimestamp
        println(" Family=" + family + " Qualifier=" + qualifier + " Value=" + value + " TimeStamp=" + tm)
      }
    })
  }

}
