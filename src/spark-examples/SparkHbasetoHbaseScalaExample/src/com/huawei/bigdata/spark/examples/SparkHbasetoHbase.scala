package com.huawei.bigdata.spark.examples

import java.io.{File, IOException}
import java.util
import java.util.Base64

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.serializer.KryoRegistrator
import com.huawei.hadoop.security.LoginUtil
import org.apache.spark.sql.SparkSession

/**
  * Get data from both hbase table: table1 and table2, calculate the sum of values and update to table2
  *
  * Before running the program, you need to add some data in hive and hbase, e.g.:
  * 1. create table in hbase with "hbase shell": create 'table1', {NAME => 'cf'}
  * 2. insert data into hbase with "hbase shell": put 'table1', 'Lily', 'cf:cid', 5
  * 3. create table in hbase with "hbase shell": create 'table2', {NAME => 'cf'}
  * 4. insert data into hbase with "hbase shell": put 'table2', 'Lily', 'cf:cid', 11
  *
  * After running the program, you can do the following to check results, e.g.:
  * 1. scan hbase table data with "hbase shell": scan 'table2', {COLUMNS => 'cf'}
  */
object SparkHbasetoHbase {

  def main(args: Array[String]) {

    val hadoopConf: Configuration  = new Configuration()
    if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))) {
      //security mode

      val userPrincipal = "sparkuser"
      val USER_KEYTAB_FILE = "user.keytab"
      val filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator
      val krbFile = filePath + "krb5.conf"
      val userKeyTableFile = filePath + USER_KEYTAB_FILE

      val ZKServerPrincipal = "zookeeper/hadoop.hadoop.com"
      val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME: String = "Client"
      val ZOOKEEPER_SERVER_PRINCIPAL_KEY: String = "zookeeper.server.principal"

      LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeyTableFile)
      LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal)
      LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf)
    }

    val spark = SparkSession.builder()
      .appName("SparkHbasetoHbase")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator")
      .getOrCreate()
    val sc = spark.sparkContext

    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath.
    val hbConf = HBaseConfiguration.create(sc.hadoopConfiguration)

    // Declare the information of the table to be queried.
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf")) //colomn family
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.getEncoder().encodeToString(proto.toByteArray)
    hbConf.set(TableInputFormat.INPUT_TABLE, "table1") //table name
    hbConf.set(TableInputFormat.SCAN, scanToString)

    //  Obtain the data in the table through the Spark interface.
    val rdd = sc.newAPIHadoopRDD(hbConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // Traverse every Partition in the HBase table1 and update the HBase table2
    // If less data, you can use rdd.foreach()
    rdd.foreachPartition(x => hBaseWriter(x))

    sc.stop()
  }

  /**
    * write to table2 in exetutor
    *
    * @param iterator partition data from table1
    */
  def hBaseWriter(iterator: Iterator[(ImmutableBytesWritable, Result)]): Unit = {
    //read hbase
    val tableName = "table2"
    val columnFamily = "cf"
    val qualifier = "cid"
    val conf = HBaseConfiguration.create()
    var table: Table = null
    var connection: Connection = null

    try {
      connection = ConnectionFactory.createConnection(conf)
      table = connection.getTable(TableName.valueOf(tableName))

      val iteratorArray = iterator.toArray
      val rowList = new util.ArrayList[Get]()
      for (row <- iteratorArray) {
        val get = new Get(row._2.getRow)
        rowList.add(get)
      }

      //get data from hbase table2
      val resultDataBuffer = table.get(rowList)

      //set data for hbase
      val putList = new util.ArrayList[Put]()
      for (i <- iteratorArray.indices) {
        val resultData = resultDataBuffer(i) //hbase2 row
        if (!resultData.isEmpty) {
          //query hbase1Value
          var hbase1Value = ""
          val it = iteratorArray(i)._2.listCells().iterator()
          while (it.hasNext) {
            val c = it.next()
            // query table1 value by colomn family and colomn qualifier
            if (columnFamily.equals(Bytes.toString(CellUtil.cloneFamily(c)))
              && qualifier.equals(Bytes.toString(CellUtil.cloneQualifier(c)))) {
              hbase1Value = Bytes.toString(CellUtil.cloneValue(c))
            }
          }

          val hbase2Value = Bytes.toString(resultData.getValue(columnFamily.getBytes, qualifier.getBytes))
          val put = new Put(iteratorArray(i)._2.getRow)

          //calculate result value
          val resultValue = hbase1Value.toInt + hbase2Value.toInt
          //set data to put
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(resultValue.toString))
          putList.add(put)
        }
      }

      if (putList.size() > 0) {
        table.put(putList)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace();
    } finally {
      if (table != null) {
        try {
          table.close()
        } catch {
          case e: IOException =>
            e.printStackTrace();
        }
      }
      if (connection != null) {
        try {
          // Close the HBase connection.
          connection.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }
  }
}

/**
  * Define serializer class.
  */
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable])
    kryo.register(classOf[org.apache.hadoop.hbase.client.Result])
    kryo.register(classOf[Array[(Any, Any)]])
    kryo.register(classOf[Array[org.apache.hadoop.hbase.Cell]])
    kryo.register(classOf[org.apache.hadoop.hbase.NoTagsKeyValue])
    kryo.register(classOf[org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionLoadStats])
  }

}
