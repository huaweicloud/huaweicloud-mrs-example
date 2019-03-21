package com.huawei.bigdata.hbase.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Phoenix Example.
 * if you would like to operate hbase by SQL, please enable it,
 * and you can refrence the url ("https://support.huaweicloud.com/devg-mrs/mrs_06_0041.html").
 */
public class PhoenixExample {

  private static String CONF_DIR = null;
  private static Configuration conf;
  private static Properties properties = new Properties();
  private static String krb5File = null;
  private static String userName = null;
  private static String userKeytabFile = null;
  private static String jaasConf = null;
  public static String HBASE_CLIENT_PROPERTIES = "hbaseclient.properties";
  private final static Logger LOG = Logger.getLogger(PhoenixExample.class.getName());

  public PhoenixExample(Configuration conf) {
    this.conf = conf;
  }

  // directly operate hbase by phoenix url.
  public static void main(String[] args) throws IOException {

    if (args.length == 1) {
      CONF_DIR = args[0] + File.separator;
      LOG.info("hbase configuration path is :" + CONF_DIR);
    }
    Configuration conf = HBaseConfiguration.create();
    ClientInfo clientInfo = new ClientInfo(CONF_DIR, HBASE_CLIENT_PROPERTIES);
    conf.addResource("hbase-site.xml");

    if (User.isHBaseSecurityEnabled(conf)) {
      userName = clientInfo.getUserName();
      if (CONF_DIR == null) {
        ClassLoader classloader = PhoenixExample.class.getClassLoader();
        krb5File = classloader.getResource(clientInfo.getKrb5File()).getPath();
        userKeytabFile = classloader.getResource(clientInfo.getUserKeytabFile()).getPath();
        jaasConf = classloader.getResource(clientInfo.getJaasConf()).getPath();
      } else {
        krb5File = CONF_DIR + clientInfo.getKrb5File();
        userKeytabFile = CONF_DIR + clientInfo.getUserKeytabFile();
        jaasConf = CONF_DIR + clientInfo.getJaasConf();
      }
      LOG.info("userName: " + userName);
      LOG.info("krb5File: " + krb5File);
      LOG.info("userKeytabFile: " + userKeytabFile);
      LOG.info("jaasConf: " + jaasConf);
      //login zk, please check "jaas.conf" and modify the parameters ("keyTab" and "principal").
      System.setProperty("java.security.krb5.conf", krb5File);
      System.setProperty("java.security.auth.login.config", jaasConf);

      properties.setProperty("hbase.myclient.keytab", userKeytabFile);
      properties.setProperty("hbase.myclient.principal", userName);
      properties.setProperty("hbase.regionserver.kerberos.principal", conf.get("hbase.regionserver.kerberos.principal"));
      properties.setProperty("hbase.master.kerberos.principal", conf.get("hbase.master.kerberos.principal"));
      properties.setProperty("hadoop.security.authentication", "kerberos");
      properties.setProperty("hbase.security.authentication", "kerberos");
    }
    PhoenixExample pe = new PhoenixExample(conf);
    pe.testSQL();
  }
  private String getURL()
  {
    String phoenix_jdbc = "jdbc:phoenix";
    String zkQuorum=conf.get("hbase.zookeeper.quorum");
    String clientPort = conf.get("hbase.zookeeper.property.clientPort", "2181");
    String hbaseNameSpaceInZK = conf.get("zookeeper.znode.parent", "/hbase");
    //phoenix URL
    StringBuilder sBuilder = new StringBuilder(phoenix_jdbc)
      .append(":")
      .append(zkQuorum)
      .append(":")
      .append(clientPort)
      .append(":")
      .append(hbaseNameSpaceInZK);
    LOG.info("the phoenix url: " + sBuilder);
    return sBuilder.toString();
  }

  protected void testSQL()
  {
    String tableName = "TEST";
    // Create table
    String createTableSQL = "CREATE TABLE IF NOT EXISTS TEST(id integer not null primary key, name varchar, account char(6), birth date)";

    // Delete table
    String dropTableSQL = "DROP TABLE TEST";

    // Insert
    String upsertSQL = "UPSERT INTO TEST VALUES(1,'John','100000', TO_DATE('1980-01-01','yyyy-MM-dd'))";

    // Query
    String querySQL = "SELECT * FROM TEST WHERE id = ?";

    // Create the Configuration instance

    // Get URL
    String URL = getURL();

    Connection conn = null;
    PreparedStatement preStat = null;
    Statement stat = null;
    ResultSet result = null;

    try
    {
      // Create Connection
      conn = DriverManager.getConnection(URL, properties);
      // Create Statement
      stat = conn.createStatement();
      // Execute Create SQL
      stat.executeUpdate(createTableSQL);
      // Execute Update SQL
      stat.executeUpdate(upsertSQL);
      conn.commit();
      // Create PrepareStatement
      preStat = conn.prepareStatement(querySQL);
      // Execute query
      preStat.setInt(1,1);
      result = preStat.executeQuery();
      // Get result
      while (result.next())
      {
        int index = 1;
        LOG.info(result.getInt(index++));
        LOG.info(result.getString(index++));
        LOG.info(result.getString(index++));
        LOG.info(result.getDate(index++));
      }
      // drop table
      stat.executeUpdate(dropTableSQL);
    }
    catch (Exception e)
    {
      LOG.error("failed to excute phoenix example, because ", e);
    }
    finally
    {
      if (null != result) {
        try {
          result.close();
        } catch (Exception e) {
          LOG.error("failed to close ResultSet, because ", e);
        }
      }
      if (null != stat) {
        try {
          stat.close();
        } catch (Exception e) {
          LOG.error("failed to close Statement, because ", e);
        }
      }
      if (null != conn) {
        try {
          conn.close();
        } catch (Exception e) {
          LOG.error("failed to close Connection, because ", e);
        }
      }
    }
  }
}