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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a simple example of putting records in HBase
 * with the bulkPut function.
 */
public final class JavaHBaseBulkPutExample {
    private JavaHBaseBulkPutExample() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("JavaHBaseBulkPutExample  " + "{tableName} {columnFamily}");
            return;
        }
        LoginUtil.loginWithUserKeytab();
        String tableName = args[0];
        String columnFamily = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkPutExample " + tableName);
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        try {
            List<String> list = new ArrayList<String>(5);
            list.add("1," + columnFamily + ",1,1");
            list.add("2," + columnFamily + ",1,2");
            list.add("3," + columnFamily + ",1,3");
            list.add("4," + columnFamily + ",1,4");
            list.add("5," + columnFamily + ",1,5");
            list.add("6," + columnFamily + ",1,6");
            list.add("7," + columnFamily + ",1,7");
            list.add("8," + columnFamily + ",1,8");
            list.add("9," + columnFamily + ",1,9");
            list.add("10," + columnFamily + ",1,10");
            JavaRDD<String> rdd = jsc.parallelize(list);

            Configuration conf = HBaseConfiguration.create();

            JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

            hbaseContext.bulkPut(rdd, TableName.valueOf(tableName), new PutFunction());
            System.out.println("Bulk put into Hbase successfully!");
        } finally {
            jsc.stop();
        }
    }

    public static class PutFunction implements Function<String, Put> {
        private static final long serialVersionUID = 1L;

        public Put call(String v) throws Exception {
            String[] cells = v.split(",");
            Put put = new Put(Bytes.toBytes(cells[0]));

            put.addColumn(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]), Bytes.toBytes(cells[3]));
            return put;
        }
    }
}
