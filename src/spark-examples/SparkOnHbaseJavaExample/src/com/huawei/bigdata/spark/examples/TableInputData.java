package com.huawei.bigdata.spark.examples;

import java.io.File;
import java.io.IOException;
import java.util.List;

import scala.Tuple4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.huawei.hadoop.security.LoginUtil;

/**
 * Input data to hbase table.
 */
public class TableInputData {
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

    // Create the configuration parameter to connect the HBase.
    SparkConf conf = new SparkConf().setAppName("CollectFemaleInfo");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());

    // Declare the information of the table.
    Table table = null;
    String tableName = "shb1";
    byte[] familyName = Bytes.toBytes("info");
    Connection connection = null;

    try {
      // Connect to the HBase.
      connection = ConnectionFactory.createConnection(hbConf);
      // Obtain the table object.
      table = connection.getTable(TableName.valueOf(tableName));
      List<Tuple4<String, String, String, String>> data = jsc.textFile(args[0]).map(
          new Function<String, Tuple4<String, String, String, String>>() {
            public Tuple4<String, String, String, String> call(String s) throws Exception {
              String[] tokens = s.split(",");

              return new Tuple4<String, String, String, String>(tokens[0], tokens[1], tokens[2], tokens[3]);
            }
          }).collect();

      Integer i = 0;
      for (Tuple4<String, String, String, String> line : data) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.addColumn(familyName, Bytes.toBytes("c11"), Bytes.toBytes(line._1()));
        put.addColumn(familyName, Bytes.toBytes("c12"), Bytes.toBytes(line._2()));
        put.addColumn(familyName, Bytes.toBytes("c13"), Bytes.toBytes(line._3()));
        put.addColumn(familyName, Bytes.toBytes("c14"), Bytes.toBytes(line._4()));
        i += 1;
        table.put(put);
      }

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (table != null) {
        try {
          // Close the HTable.
          table.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (connection != null) {
        try {
          // Close the HBase connection.
          connection.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      jsc.stop();
    }
  }
}
