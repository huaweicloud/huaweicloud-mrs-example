package com.huawei.bigdata.spark.examples;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.huawei.hadoop.security.LoginUtil;

/**
 * Create table in hbase.
 */
public class TableCreation {
  public static void main(String[] args) throws IOException {
    Configuration hadoopConf = new Configuration();
    if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))){
      //security mode

      final String userPrincipal = "sparkuser";
      final String USER_KEYTAB_FILE = "user.keytab";
      String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
      String krbFile = filePath + "krb5.conf";
      String userKeyTableFile = filePath + USER_KEYTAB_FILE;

      String ZKServerPrincipal = "zookeeper/hadoop.hadoop.com";
      String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
      String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

      LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeyTableFile);
      LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal);
      LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf);;
    }

    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath
    SparkConf conf = new SparkConf().setAppName("CollectFemaleInfo");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());
    // Create the connection channel to connect the HBase.
    Connection connection = ConnectionFactory.createConnection(hbConf);

    // Declare the description of the table.
    TableName userTable = TableName.valueOf("shb1");
    HTableDescriptor tableDescr = new HTableDescriptor(userTable);
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes()));

    // Create a table.
    System.out.println("Creating table shb1. ");
    Admin admin = connection.getAdmin();
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable);
      admin.deleteTable(userTable);
    }
    admin.createTable(tableDescr);

    connection.close();
    jsc.stop();
    System.out.println("Done!");
  }
}
