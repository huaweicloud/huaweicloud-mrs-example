/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.bigdata.spark.examples.hbasecontext;

import com.huawei.hadoop.security.LoginUtil;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions.*;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * This is a simple example of using the mapPartitions
 * method with a HBase connection
 * some data need be accessable on hbase in advance
 */
public class JavaHBaseMapPartitionExample {
    public static void main(String args[]) throws IOException {
        if (args.length < 1) {
            System.out.println("JavaHBaseMapPartitionExample {tableName} is missing an argument");
            return;
        }
        LoginUtil.loginWithUserKeytab();
        final String tableName = args[0];
        SparkConf sparkConf = new SparkConf().setAppName("HBaseMapPartitionExample " + tableName);
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        try {
            List<byte[]> list = new ArrayList();
            list.add(Bytes.toBytes("1"));
            list.add(Bytes.toBytes("2"));
            list.add(Bytes.toBytes("3"));
            list.add(Bytes.toBytes("4"));
            list.add(Bytes.toBytes("5"));
            JavaRDD<byte[]> rdd = jsc.parallelize(list);

            Configuration hbaseconf = HBaseConfiguration.create();
            JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hbaseconf);

            JavaRDD getrdd =
                    hbaseContext.mapPartitions(
                            rdd,
                            new FlatMapFunction<Tuple2<Iterator<byte[]>, Connection>, Object>() {
                                public Iterator call(Tuple2<Iterator<byte[]>, Connection> t) throws Exception {
                                    Table table = t._2.getTable(TableName.valueOf(tableName));
                                    // go through rdd
                                    List<String> list = new ArrayList<String>();
                                    while (t._1.hasNext()) {
                                        byte[] bytes = t._1.next();
                                        Result result = table.get(new Get(bytes));
                                        Iterator<Cell> it = result.listCells().iterator();
                                        StringBuilder sb = new StringBuilder();
                                        sb.append(Bytes.toString(result.getRow()) + ":");
                                        while (it.hasNext()) {
                                            Cell cell = it.next();
                                            String column = Bytes.toString(cell.getQualifierArray());
                                            if (column.equals("counter")) {
                                                sb.append(
                                                        "(" + column + "," + Bytes.toLong(cell.getValueArray()) + ")");
                                            } else {
                                                sb.append(
                                                        "("
                                                                + column
                                                                + ","
                                                                + Bytes.toString(cell.getValueArray())
                                                                + ")");
                                            }
                                        }
                                        list.add(sb.toString());
                                    }
                                    return list.iterator();
                                }
                            });
            List<byte[]> resultList = getrdd.collect();
            if (null == resultList || 0 == resultList.size()) {
                System.out.println("Nothing matches!");
            } else {
                for (int i = 0; i < resultList.size(); i++) {
                    System.out.println(resultList.get(i));
                }
            }

        } finally {
            jsc.stop();
        }
    }
}
