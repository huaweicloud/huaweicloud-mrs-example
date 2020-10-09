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
package com.huawei.bigdata.spark.examples.streaming;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is a simple example of putting kafka streamData into HBase
 * with the streamBulkPut function.
 */
public class SparkOnStreamingToHbaseExample {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            printUsage();
        }

        String checkPointDir = args[0];
        String topics = args[1];
        String brokers = args[2];
        String tableName = args[3];

        SparkConf conf = new SparkConf();
        conf.setAppName("SparkOnStreamingToHbase_Java");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.milliseconds(5000));

        Configuration config = HBaseConfiguration.create();
        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, config);

        if (!"nocp".equals(checkPointDir)) {
            jssc.checkpoint(checkPointDir);
        }

        String[] topicArray = topics.split(",");
        Set<String> topicSet = new HashSet<String>(Arrays.asList(topicArray));

        Map kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", brokers);

        JavaPairInputDStream<String, String> messages =
                KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicSet);

        JavaDStream<String> lines =
                messages.map(
                        new Function<Tuple2<String, String>, String>() {
                            @Override
                            public String call(Tuple2<String, String> tuple2) {
                                return tuple2._2();
                            }
                        });

        hbaseContext.streamBulkPut(lines, TableName.valueOf(tableName), new PutFunction());

        jssc.start();
        jssc.awaitTermination();
    }

    public static class PutFunction implements Function<String, Put> {
        private static final long serialVersionUID = 1L;

        public Put call(String v) throws Exception {
            if (v.length() > 0) {
                Put put = new Put(Bytes.toBytes(v));
                put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cid"), Bytes.toBytes(v));
                return put;
            } else {
                return null;
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage: {checkPointDir} {topic} {brokerList} {tableName}");
        System.exit(1);
    }
}
