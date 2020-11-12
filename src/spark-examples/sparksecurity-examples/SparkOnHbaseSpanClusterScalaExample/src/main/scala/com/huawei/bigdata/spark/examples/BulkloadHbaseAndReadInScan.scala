package com.huawei.bigdata.spark.examples

import java.util.Base64

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.tool.BulkLoadHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object BulkloadHbaseAndReadInScan extends ExportToHbaseTask {

  var sourceDataFrame: DataFrame = _

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("ExportToHBaseInBulkload")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    hbaseConfigure  = new HbaseConfigureBuilder()
        .setTableName("outTablePut1")
        .setSourceHdfsPath("/tmp/bulkload-1000.csv")
        .setSaveHfilePath("hdfs://test/tmp/hbase/outPutTable1")
        .setCloumnfamily("family")
        .setDefKey("id")
        .build()
    runHbaseTask(spark)
  }



  /**
   * Initialization information
   */
  override def init(spark: SparkSession): Unit = {
    setHbaseConf()
    connection = ConnectionFactory.createConnection(conf)
    admin = connection.getAdmin

  }

  def setHbaseConf(): Unit = {
    conf  = HBaseConfiguration.create()
    conf.set("hbase.mapreduce.hfileoutputformat.table.name", hbaseConfigure.tableName)
    conf.set("hbase.unsafe.stream.capability.enforce", "false")
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "400")
  }

  /**
   * write Hbase In Bulkload
   */
  override def writeToHbase(spark: SparkSession): Unit = {
    getDataset(spark)
    val hfileRDD: RDD[(ImmutableBytesWritable, KeyValue)] = getHFileRDD(spark)
    saveHfile(hfileRDD)
    loadHFileToHbase()
  }

  /**
   * Get source data table
   */
  def getDataset(spark: SparkSession) = {
    sourceDataFrame = spark.read.option("header","true").csv(hbaseConfigure.sourceHdfsPath)
  }

  /**
   * Change dataset to Hfile
   */
  def getHFileRDD(spark: SparkSession): RDD[(ImmutableBytesWritable, KeyValue)] = {
    import spark.implicits._
    val key = hbaseConfigure.defKey
    val family = hbaseConfigure.columnfamily
    val columnsName: Array[String] = sourceDataFrame.columns

    val rddResult: RDD[(ImmutableBytesWritable, Seq[KeyValue])] = sourceDataFrame
      .repartition(3, $"$key")
      .rdd
      .map(row =>
      {
        var kvlist: Seq[KeyValue] = List()
        var kv: KeyValue = null
        val rowKey = Bytes.toBytes(row.getAs[String](key) + "")
        val immutableRowKey: ImmutableBytesWritable = new ImmutableBytesWritable(rowKey)
        for (i <- 0 to (columnsName.length - 1)) {
          var value: Array[Byte] = null
          try {
            value = Bytes.toBytes(row.getAs[String](columnsName(i)))
          }
          catch {
            case e: ClassCastException =>
              value = Bytes.toBytes(row.getAs[BigInt](columnsName(i)) + "")
            case e: Exception => e.printStackTrace()
          }
          kv = new KeyValue(rowKey, family.getBytes, Bytes.toBytes(columnsName(i)), value)
          kvlist = kvlist :+ kv
        }

        (immutableRowKey, kvlist)
      })

    val hfileRDD: RDD[(ImmutableBytesWritable, KeyValue)] = rddResult.flatMapValues(_.iterator)

    hfileRDD
  }


  /**
   * save hfile in other hdfs cluster
   * @param hfileRDD
   */
  def saveHfile(hfileRDD: RDD[(ImmutableBytesWritable, KeyValue)]) = {
    deleteOldHfile(hbaseConfigure.savePath)

    hfileRDD
      .sortBy(row => (row._1, row._2.getKeyString), true) //要保持 整体有序
      .saveAsNewAPIHadoopFile(hbaseConfigure.savePath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        conf)
  }

  /**
   * HFile do bulkload to HBase
   */
  def loadHFileToHbase() = {

    val load: BulkLoadHFiles = BulkLoadHFiles.create(conf)
    val table: Table = connection.getTable(TableName.valueOf(hbaseConfigure.tableName))
    val regionLocator: RegionLocator = connection.getRegionLocator(TableName.valueOf(hbaseConfigure.tableName))

    val job: Job = Job.getInstance(conf)
    job.setJobName(s"$hbaseConfigure.tableName LoadIncrementalHFiles")
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)
    load.bulkLoad(TableName.valueOf(hbaseConfigure.tableName),new Path(hbaseConfigure.savePath))
  }


  /**
   * remove hfile in hdfs
   */
  def deleteOldHfile(url: String) {
    val path: Path = new Path(url)
    val hdfs: FileSystem = path.getFileSystem(new Configuration())
    if (hdfs.exists(path))
    {
      hdfs.delete(path, true)
    }
  }

  /**
    * read hbase
    */
  override def readFromHbase(spark: SparkSession): Unit = {
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes(hbaseConfigure.columnfamily))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.getEncoder.encodeToString(proto.toByteArray);
    conf.set(TableInputFormat.INPUT_TABLE, hbaseConfigure.tableName)
    conf.set(TableInputFormat.SCAN, scanToString)

    val rdd = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd.collect().foreach(result =>
    {
      val key = result._1.toString
      val it = result._2.listCells().iterator()
      while (it.hasNext) {
        val cell = it.next()
        val family = Bytes.toString(CellUtil.cloneFamily(cell))
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        val tm = cell.getTimestamp
        logger.info(" Family=" + family + " Qualifier=" + qualifier + " Value=" + value + " TimeStamp=" + tm)
      }
    })
    logger.info("Readding table : "+ hbaseConfigure.tableName + " endding ")
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
