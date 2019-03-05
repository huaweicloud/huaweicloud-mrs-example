package com.huawei.bigdata.spark.examples

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.huawei.hadoop.security.LoginUtil

/**
  * Create table in hbase.
  */
object TableCreation {
  def main(args: Array[String]): Unit = {
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

    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath.
    val conf: SparkConf = new SparkConf
    val sc: SparkContext = new SparkContext(conf)
    val hbConf: Configuration = HBaseConfiguration.create(sc.hadoopConfiguration)

    // Create the connection channel to connect the HBase
    val connection: Connection = ConnectionFactory.createConnection(hbConf)

    // Declare the description of the table
    val userTable = TableName.valueOf("shb1")
    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes))

    // Create a table
    println("Creating table shb1. ")
    val admin = connection.getAdmin
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)

    connection.close()
    sc.stop()
    println("Done!")
  }
}
