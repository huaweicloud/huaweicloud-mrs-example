package com.huawei.bigdata.spark

import java.io.IOException
import java.security.PrivilegedExceptionAction

import com.huawei.bigdata.utils.{HBaseUtil, KerberosTableInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * Read and write HBase example with kerberos
 *
 * @since 2021-01-25
 */
object SparkHBase {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkUtil.getSparkSession()

    readHBase(sparkSession)

    writeToHBase(sparkSession)

    readHBase(sparkSession)

    sparkSession.close()

  }

  /**
   * Read HBase
   *
   * @param sparkSession SparkSession instance
   */
  def readHBase(sparkSession: SparkSession): Unit = {

    val config = HBaseConfiguration.create
    config.addResource("hbase-site.xml")
    config.set(KerberosTableInputFormat.INPUT_TABLE,"SparkHBase")

    val HBaseRDD = sparkSession.sparkContext.newAPIHadoopRDD(config,
      classOf[KerberosTableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import sparkSession.implicits._

    HBaseRDD.map({case (_,result) =>
      val id = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("cf1".getBytes, "name".getBytes))
      val age = Bytes.toString(result.getValue("cf1".getBytes, "age".getBytes))
      (id,name,age)
    }).toDF("id","name","age").show()

  }

  /**
   * Write HBase
   *
   * @param sparkSession SparkSession instance
   */
  def writeToHBase(sparkSession: SparkSession): Unit = {

    val tableName = "SparkHBase"

    val dataRDD = sparkSession.sparkContext.makeRDD(Array("02,Min,22", "03,Mike,32", "04,Lucy,36", "05,Well,45"))

    dataRDD.foreachPartition(p => {
      SparkUtil.login()
      UserGroupInformation.getLoginUser
        .doAs(
          new PrivilegedExceptionAction[Void]() {
            @throws[Exception]
            override def run: Void = {
              val conn = getConnect
              val resTable = TableName.valueOf(tableName)
              val table = conn.getTable(resTable)
              p.foreach(r => {
                val arr = r.split(",")
                val put = new Put(Bytes.toBytes(arr(0)))
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(arr(2)))
                Try(table.put(put)).getOrElse(table.close())
              })
              table.close()
              conn.close()
              null
            }
          }
        )
    })
  }

  /**
   * Get HBase Connection
   * @return HBase Connection
   */
  def getConnect: Connection = {
    try {
      val config = HBaseConfiguration.create
      config.addResource("hbase-site.xml")
      HBaseUtil.zkSslUtil(config)
      val connection = ConnectionFactory.createConnection(config)
      connection
    } catch {
      case e:IOException =>
        throw new RuntimeException(e)
      case e2:Exception =>
        throw new RuntimeException(e2)
    }
  }

}
