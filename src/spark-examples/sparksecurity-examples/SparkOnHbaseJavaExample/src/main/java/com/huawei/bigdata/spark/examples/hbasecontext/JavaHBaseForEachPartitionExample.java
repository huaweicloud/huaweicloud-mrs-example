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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This is a simple example of using the foreachPartition
 * method with a HBase connection
 */
public final class JavaHBaseForEachPartitionExample {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("JavaHBaseForEachPartitionExample {tableName} {columnFamily}");
            return;
        }
        LoginUtil.loginWithUserKeytab();
        final String tableName = args[0];
        final String columnFamily = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkGetExample " + tableName);
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        try {
            List<byte[]> list = new ArrayList<byte[]>(5);
            list.add(Bytes.toBytes("1"));
            list.add(Bytes.toBytes("2"));
            list.add(Bytes.toBytes("3"));
            list.add(Bytes.toBytes("4"));
            list.add(Bytes.toBytes("5"));

            JavaRDD<byte[]> rdd = jsc.parallelize(list);
            Configuration conf = HBaseConfiguration.create();

            JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
            hbaseContext.foreachPartition(
                    rdd,
                    new VoidFunction<Tuple2<Iterator<byte[]>, Connection>>() {
                        /**
                         * write data to hbase table
                         *
                         * @param conn connection object
                         * @throws Exception
                         */
                        public void call(Tuple2<Iterator<byte[]>, Connection> conn) throws Exception {
                            Connection con = conn._2;
                            Iterator<byte[]> it = conn._1;
                            BufferedMutator buf = con.getBufferedMutator(TableName.valueOf(tableName));
                            while (it.hasNext()) {
                                byte[] bts = it.next();
                                Put put = new Put(bts);
                                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("cid"), bts);
                                buf.mutate(put);
                            }
                            buf.flush();
                            buf.close();
                        }
                    });
        } finally {
            jsc.stop();
        }
    }
}
