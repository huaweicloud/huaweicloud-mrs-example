package com.huawei.bigdata.spark.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, Connection, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

trait ExportToHbaseTask
{

  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)
  var hbaseConfigure: HbaseConfigure = _
  var admin: Admin = _
  var connection : Connection = _
  var conf: Configuration = _



  /**
   * Task template
   *
   */
  def runHbaseTask(spark: SparkSession): Unit =
  {
    try
    {
      init(spark)
      creatHbaseTable()
      writeToHbase(spark)
      readFromHbase(spark)
      deleteHbaseTable()
      endWork(spark)
    }
    catch
    {
      case e: Exception => e.printStackTrace
    }
  }


  /**
   * Initialization information
   */
  def init(spark: SparkSession)

  /**
    * create hbase table
    */
  def creatHbaseTable(): Unit =
  {
    val tableName = TableName.valueOf(hbaseConfigure.tableName)
    if (admin.tableExists(tableName))
    {
      logger.info(s"table $hbaseConfigure.tableName existed")
    }
    else
    {
      val tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      val family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(hbaseConfigure.columnfamily)).build
      tableDescriptor.setColumnFamily(family)
      admin.createTable(tableDescriptor.build())
      logger.info(s"$hbaseConfigure.tableName create success!")
    }
  }


  /**
   * write hbase
   */
  def writeToHbase(spark: SparkSession)

  /**
    * read hbase
    */
  def readFromHbase(spark: SparkSession)

  /**
    * delete hbase table
    */
  def deleteHbaseTable(): Unit =
  {
    admin.disableTable(TableName.valueOf(hbaseConfigure.tableName))
    admin.deleteTable(TableName.valueOf(hbaseConfigure.tableName))
  }

  /**
    * Handle follow-up work
    */
  def endWork(spark: SparkSession): Unit =
  {
    spark.stop()
  }

}
