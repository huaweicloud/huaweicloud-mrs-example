package com.huawei.bigdata.spark.examples

import java.util.Base64

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}

object PutHbaseAndReadInGet extends ExportToHbaseTask {
  //save scan result for get hbase
  var rowkeyResult: List[String] = List()
  var sourceDataFrame: DataFrame = _

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("ExportToHBaseInPut")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    hbaseConfigure  = new HbaseConfigureBuilder()
      .setTableName("outTablePut")
      .setSourceHdfsPath("/tmp/bulkload-1000.csv")
      .setSaveHfilePath("hdfs://test/tmp/hbase/outPutTable")
      .setCloumnfamily("family")
      .setDefKey("id")
      .build()
    runHbaseTask(spark)

  }



  /**
   * Initialization information
    *
   */
  override def init(spark: SparkSession): Unit = {
    val conf: Configuration = HBaseConfiguration.create()
    connection = ConnectionFactory.createConnection(conf)
    admin = connection.getAdmin
  }

  /**
   * write hbase in put
   */
  override def writeToHbase(spark: SparkSession): Unit = {
    getDataset(spark)
    writeToHbaseData(spark)
  }

  /**
    * writer to table
    */
  def writeToHbaseData(spark: SparkSession): Unit = {
    import scala.collection.JavaConverters._
    val familyName = Bytes.toBytes(hbaseConfigure.columnfamily)
    val columnsNames: Array[String] = sourceDataFrame.columns
    val key = hbaseConfigure.defKey
    val columnFamily = hbaseConfigure.columnfamily
    val tableName = hbaseConfigure.tableName
    var putList: List[Put] = List()
    sourceDataFrame.rdd.foreachPartition(partition =>
    {
        val conf: Configuration = HBaseConfiguration.create()
        val connection = ConnectionFactory.createConnection(conf)
        var table: Table = connection.getTable(TableName.valueOf(tableName))
        partition.foreach(row =>
        {
          val rowKey = Bytes.toBytes(row.getAs[String](key) + "")
          val rowPut: Put = new Put(rowKey);
          for (i <- 0 to (columnsNames.length - 1))
          {
            var value: Array[Byte] = null
            value = Bytes.toBytes(row.getAs[String](columnsNames(i)))
            rowPut.addColumn(familyName,Bytes.toBytes(columnsNames(i)),value)
          }
          putList = putList :+ rowPut
        })
      table.put(putList.asJava)
    })

  }
  /**
   * Get source data table
   */
  def getDataset(spark: SparkSession) = {
    sourceDataFrame = spark.read.option("header","true").csv(hbaseConfigure.sourceHdfsPath)
  }
  /**
    * read hbase
    */
  override def readFromHbase(spark: SparkSession): Unit = {
    readFromHbaseInScan(spark)
    readFromHbaseInGet()
  }
  /**
    * read hbase in scan
    */
def readFromHbaseInScan(spark: SparkSession): Unit = {
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes(hbaseConfigure.columnfamily))
    scan.setBatch(20)
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.getEncoder.encodeToString(proto.toByteArray);
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, hbaseConfigure.tableName)
    conf.set(TableInputFormat.SCAN, scanToString)

    val rdd = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd.collect().foreach(row =>
    {
      val key = row._1.toString
      val it = row._2.listCells().iterator()
      while (it.hasNext) {
        val cell = it.next()
        val family = Bytes.toString(CellUtil.cloneFamily(cell))
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        val tm = cell.getTimestamp
        logger.info(" Family=" + family + " Qualifier=" + qualifier + " Value=" + value + " TimeStamp=" + tm)
      }
      rowkeyResult = rowkeyResult :+ key
    })
  }

  /**
    * read hbase in get
    */
  def readFromHbaseInGet(): Unit = {
    import scala.collection.JavaConverters._
    val htable = connection.getTable(TableName.valueOf(hbaseConfigure.tableName))
    var getlist: List[Get] = List()
    for(row <- rowkeyResult)
    {
      val get: Get = new Get(Bytes.toBytes(row))
      getlist = getlist :+ get
    }

    val results : Array[Result] = htable.get(getlist.asJava)
    for(result <- results)
      {
        for (cell <- result.rawCells)
        {
          val family = Bytes.toString(CellUtil.cloneFamily(cell))
          val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          val tm = cell.getTimestamp
          logger.info(" Family=" + family + " Qualifier=" + qualifier + " Value=" + value + " TimeStamp=" + tm)
        }
      }
  }
  /**
    * Handle follow-up work
    */
  override def endWork(spark: SparkSession): Unit = {
    connection.close()
    admin.close()
    super.endWork(spark)
  }
}
