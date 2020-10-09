package com.huawei.bigdata.spark.examples.streaming;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.Serializable;
import java.util.*;

public class SparkOnStreamingToHbasePythonKafka08 implements Serializable {
    public void streamingToHbase(
            JavaSparkContext jsc, String checkPointDir, String topics, String brokers, String tableName)
            throws Exception {
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
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
