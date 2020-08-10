package com.huawei.bigdata.spark.examples

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement}

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer


/**
  * Example code for connecting spark JDBCServer
  *
  * Before Running:
  * 1. get your hive-site.xml from MRS client to resources folder
  * 2. get your core-site.xml from MRS client to resources folder (only for kerberos enabled cluster)
  * 3. put user.keytab in the conf/user.keytab relative to the running directory and set correct principal in the code (only for kerberos enabled cluster)
  * 4. put some data into /home/data folder on hdfs
  * 5. run using command like shown below
  */
// java -cp /path/to/SparkThriftServerExample-1.0.jar:/opt/client/Spark/spark/jars/* com.huawei.bigdata.spark.examples.ThriftServerQueriesTest

object ThriftServerQueriesTest {
  def main(args: Array[String]): Unit = {

    val connectUrl = {
      val hadoopConf: Configuration  = new Configuration()
      if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))) {
        //      security mode
        val userPrincipal = "sparkuser"
        val userKeytab = "user.keytab"
        val filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator
        val krbFile = filePath + "krb5.conf"
        val userKeyTableFile = filePath + userKeytab

        val ZKServerPrincipal = "zookeeper/hadoop.hadoop.com"
        val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME: String = "Client"
        val ZOOKEEPER_SERVER_PRINCIPAL_KEY: String = "zookeeper.server.principal"
        LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeyTableFile)
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal)
        LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf)

        val securityConfig = s"saslQop=auth-conf;principal=spark/hadoop.hadoop.com@HADOOP.COM;auth=KERBEROS;user.principal=$userPrincipal;user.keytab=$userKeyTableFile"

        s"jdbc:hive2://ha-cluster/default;$securityConfig;"
      } else {
        "jdbc:hive2://ha-cluster/default;"
      }
    }

    println(connectUrl)
    val sqlList = new ArrayBuffer[String]
    sqlList += "CREATE TABLE IF NOT EXISTS CHILD (NAME STRING, AGE INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    sqlList += "LOAD DATA INPATH '/home/data' INTO TABLE CHILD"
    sqlList += "SELECT * FROM child"
    sqlList += "DROP TABLE child"

    executeSql(connectUrl, sqlList.toArray)
  }

  def executeSql(url: String, sqls: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance()
    var connection: Connection = null
    var statement: PreparedStatement = null
    try {
      connection = DriverManager.getConnection(url)
      for (sql <- sqls) {
        println(s"---- Begin executing sql: $sql ----")
        statement = connection.prepareStatement(sql)

        val result = statement.executeQuery()

        val resultMetaData = result.getMetaData
        val colNum = resultMetaData.getColumnCount
        for (i <- 1 to colNum) {
          print(resultMetaData.getColumnLabel(i) + "\t")
        }
        println()

        while (result.next()) {
          for (i <- 1 to colNum) {
            print(result.getString(i) + "\t")
          }
          println()
        }
        println(s"---- Done executing sql: $sql ----")
      }
    } finally {
      if (null != statement) {
        statement.close()
      }

      if (null != connection) {
        connection.close()
      }
    }
  }


}
