package com.huawei.bigdata.spark.examples;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.*;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * calculate data from hbase1/hbase2,then update to hbase2
 */
public class SparkHbasetoHbase {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("SparkHbasetoHbase");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "com.huawei.bigdata.spark.examples.MyRegistrator");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the
        // classpath.
        Configuration hbConf = HBaseConfiguration.create(jsc.hadoopConfiguration());

        // Declare the information of the table to be queried.
        Scan scan = new org.apache.hadoop.hbase.client.Scan();
        scan.addFamily(Bytes.toBytes("cf")); // colomn family
        hbConf.set(TableInputFormat.INPUT_TABLE, "table1"); // table name
        hbConf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(new Scan()));

        // Obtain the data in the table through the Spark interface.
        JavaPairRDD rdd =
                jsc.newAPIHadoopRDD(hbConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // Traverse every Partition in the HBase table1 and update the HBase table2
        // If less data, you can use rdd.foreach()
        rdd.foreachPartition(
                new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
                    public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> iterator) throws Exception {
                        hBaseWriter(iterator);
                    }
                });

        jsc.stop();
    }

    /**
     * write to table2 in exetutor
     *
     * @param iterator partition data from table1
     */
    private static void hBaseWriter(Iterator<Tuple2<ImmutableBytesWritable, Result>> iterator) throws IOException {
        // read hbase
        String tableName = "table2";
        String columnFamily = "cf";
        String qualifier = "cid";
        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        Table table = null;

        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));

            List<Get> rowList = new ArrayList<Get>();
            List<Tuple2<ImmutableBytesWritable, Result>> table1List =
                    new ArrayList<Tuple2<ImmutableBytesWritable, Result>>();
            while (iterator.hasNext()) {
                Tuple2<ImmutableBytesWritable, Result> item = iterator.next();
                Get get = new Get(item._2().getRow());
                table1List.add(item);
                rowList.add(get);
            }

            // get data from hbase table2
            Result[] resultDataBuffer = table.get(rowList);

            // set data for hbase
            List<Put> putList = new ArrayList<Put>();
            for (int i = 0; i < resultDataBuffer.length; i++) {
                Result resultData = resultDataBuffer[i]; // hbase2 row
                if (!resultData.isEmpty()) {
                    // query hbase1Value
                    String hbase1Value = "";
                    Iterator<Cell> it = table1List.get(i)._2().listCells().iterator();
                    while (it.hasNext()) {
                        Cell c = it.next();
                        // query table1 value by colomn family and colomn qualifier
                        if (columnFamily.equals(Bytes.toString(CellUtil.cloneFamily(c)))
                                && qualifier.equals(Bytes.toString(CellUtil.cloneQualifier(c)))) {
                            hbase1Value = Bytes.toString(CellUtil.cloneValue(c));
                        }
                    }

                    String hbase2Value =
                            Bytes.toString(resultData.getValue(columnFamily.getBytes(), qualifier.getBytes()));
                    Put put = new Put(table1List.get(i)._2().getRow());

                    // calculate result value
                    int resultValue = Integer.parseInt(hbase1Value) + Integer.parseInt(hbase2Value);
                    // set data to put
                    put.addColumn(
                            Bytes.toBytes(columnFamily),
                            Bytes.toBytes(qualifier),
                            Bytes.toBytes(String.valueOf(resultValue)));
                    putList.add(put);
                }
            }

            if (putList.size() > 0) {
                table.put(putList);
            }
        } catch (IOException e) {
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
