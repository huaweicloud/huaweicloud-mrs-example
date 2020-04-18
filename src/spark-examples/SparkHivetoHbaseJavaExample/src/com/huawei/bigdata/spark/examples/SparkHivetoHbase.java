package com.huawei.bigdata.spark.examples;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.*;

import com.huawei.hadoop.security.LoginUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * calculate data from hive/hbase,then update to hbase
 */
public class SparkHivetoHbase {

  public static void main(String[] args) throws Exception {
    Configuration hadoopConf = new Configuration();
    if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))){
      //security mode

      final String userPrincipal = "sparkuser";
      final String USER_KEYTAB_FILE = "user.keytab";
      String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
      String krbFile = filePath + "krb5.conf";
      String userKeyTableFile = filePath + USER_KEYTAB_FILE;

      LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf);
    }

    // Obtain the data in the table through the Spark interface.
    SparkConf conf = new SparkConf().setAppName("SparkHivetoHbase");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(jsc);
    Dataset<Row> dataFrame = sqlContext.sql("select name, account from person");

    // Traverse every Partition in the hive table and update the hbase table
    // If less data, you can use rdd.foreach()
    dataFrame.toJavaRDD().foreachPartition(
      new VoidFunction<Iterator<Row>>() {
        public void call(Iterator<Row> iterator) throws Exception {
          hBaseWriter(iterator);
        }
      }
    );

    jsc.stop();
  }

  /**
   * write to hbase table in exetutor
   *
   * @param iterator partition data from hive table
   */
  private static void hBaseWriter(Iterator<Row> iterator) throws IOException {
    // read hbase
    String tableName = "table2";
    String columnFamily = "cf";
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf(tableName));

    try {
      connection = ConnectionFactory.createConnection(conf);
      table = connection.getTable(TableName.valueOf(tableName));

      List<Row> table1List = new ArrayList<Row>();
      List<Get> rowList = new ArrayList<Get>();
      while (iterator.hasNext()) {
        Row item = iterator.next();
        // set the put condition
        Get get = new Get(item.getString(0).getBytes());
        table1List.add(item);
        rowList.add(get);
      }

      // get data from hbase table
      Result[] resultDataBuffer = table.get(rowList);

      // set data for hbase
      List<Put> putList = new ArrayList<Put>();
      for (int i = 0; i < resultDataBuffer.length; i++) {
        // hbase row
        Result resultData = resultDataBuffer[i];
        if (!resultData.isEmpty()) {
          // get hiveValue
          int hiveValue = table1List.get(i).getInt(1);

          // get hbaseValue by column Family and colomn qualifier
          String hbaseValue = Bytes.toString(resultData.getValue(columnFamily.getBytes(), "cid".getBytes()));
          Put put = new Put(table1List.get(i).getString(0).getBytes());

          // calculate result value
          int resultValue = hiveValue + Integer.valueOf(hbaseValue);

          // set data to put
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("cid"), Bytes.toBytes(String.valueOf(resultValue)));
          putList.add(put);
        }
      }

      if (putList.size() > 0) {
        table.put(putList);
      }
    }catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (table != null) {
        try {
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
    }
  }
}
