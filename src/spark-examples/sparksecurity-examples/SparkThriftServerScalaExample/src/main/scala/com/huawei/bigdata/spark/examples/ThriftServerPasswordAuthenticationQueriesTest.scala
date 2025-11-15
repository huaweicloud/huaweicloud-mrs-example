package com.huawei.bigdata.spark.examples

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util.Properties

object ThriftServerPasswordAuthenticationQueriesTest {
  private val properties = new Properties

  private def init(): Unit = {
    properties.setProperty("user", "YourUserName") // need to change the value based on the cluster information
    // Hard-coded password or plaintext password in code poses significant security risks. Encrypt and store them in configuration files or environment variables and decrypt them when needed.
    properties.setProperty("password", "password")
  }

  def main(args: Array[String]): Unit = {
    var connection: Connection = null
    var resultSet: ResultSet = null
    var statement: PreparedStatement = null
    // need to change the value based on the cluster information
    val jdbcUrl = "jdbc:hive2://192.168.42.247:24002,192.168.42.233:24002,192.168.42.224:24002/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=sparkthriftserver;saslQop=auth-conf;auth=KERBEROS;"
    // When hive.server2.use.SSL=true, you need to add the ssl=true parameter to the JDBC URL.
    // jdbcUrl = jdbcUrl + "ssl=true"
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    try {
      init()
      val sql = "show tables"
      connection = DriverManager.getConnection(jdbcUrl, properties)
      statement = connection.prepareStatement(sql.trim)
      resultSet = statement.executeQuery
      val colNum = resultSet.getMetaData.getColumnCount
      while (resultSet.next) for (i <- 1 to colNum) {
        System.out.println(resultSet.getString(i) + "\t")
      }
    } catch {
      case e@(_: SQLException | _: ClassNotFoundException) =>
        e.printStackTrace()
    } finally {
      if (resultSet != null) try resultSet.close()
      catch {
        case e: SQLException =>
          e.printStackTrace()
      }
      if (statement != null) try statement.close()
      catch {
        case e: SQLException =>
          e.printStackTrace()
      }
      if (connection != null) try connection.close()
      catch {
        case e: SQLException =>
          e.printStackTrace()
      }
    }
  }

}
