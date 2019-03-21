package com.huawei.bigdata.spark.examples;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.huawei.hadoop.security.LoginUtil;

/**
 * Get data from table.
 */
public class TableOutputData {
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

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    System.setProperty("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator");

    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath.
    SparkConf conf = new SparkConf().setAppName("CollectFemaleInfo");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());

    // Declare the information of the table to be queried.
    Scan scan = new org.apache.hadoop.hbase.client.Scan();
    scan.addFamily(Bytes.toBytes("info"));
    org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
    String scanToString = Base64.encodeBytes(proto.toByteArray());
    hbConf.set(TableInputFormat.INPUT_TABLE, "shb1");
    hbConf.set(TableInputFormat.SCAN, scanToString);

    // Obtain the data in the table through the Spark interface.
    JavaPairRDD rdd = jsc.newAPIHadoopRDD(hbConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

    // Traverse every row in the HBase table and print the results.
    List<Tuple2<ImmutableBytesWritable, Result>> rddList = rdd.collect();
    for (int i = 0; i < rddList.size(); i++) {
      Tuple2<ImmutableBytesWritable, Result> t2 = rddList.get(i);
      ImmutableBytesWritable key = t2._1();
      Iterator<Cell> it = t2._2().listCells().iterator();
      while (it.hasNext()) {
        Cell c = it.next();
        String family = Bytes.toString(CellUtil.cloneFamily(c));
        String qualifier = Bytes.toString(CellUtil.cloneQualifier(c));
        String value = Bytes.toString(CellUtil.cloneValue(c));
        Long tm = c.getTimestamp();
        System.out.println(" Family=" + family + " Qualifier=" + qualifier + " Value=" + value + " TimeStamp=" + tm);
      }
    }
    jsc.stop();
  }
}
