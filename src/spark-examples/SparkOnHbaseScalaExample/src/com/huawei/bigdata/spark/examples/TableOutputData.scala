package com.huawei.bigdata.spark.examples

import java.io.File

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoRegistrator
import com.huawei.hadoop.security.LoginUtil

/**
  * Get data from table.
  */
object TableOutputData {
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

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator")

    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath.
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hbConf = HBaseConfiguration.create(sc.hadoopConfiguration)

    // Declare the information of the table to be queried.
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("info"))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbConf.set(TableInputFormat.INPUT_TABLE, "shb1")
    hbConf.set(TableInputFormat.SCAN, scanToString)

    //  Obtain the data in the table through the Spark interface.
    val rdd = sc.newAPIHadoopRDD(hbConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // Traverse every row in the HBase table and print the results
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

    sc.stop()
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

