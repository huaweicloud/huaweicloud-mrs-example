package com.huawei.bigdata.spark.examples.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.io.Serializable;
import java.util.*;

public class SparkOnStreamingToHbasePythonKafka10 implements Serializable {
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

        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "testGroup");
        kafkaParams.put("auto.offset.rest", "latest");
        kafkaParams.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        kafkaParams.put("enable.auto.commit", true);

        LocationStrategy locationStrategy = LocationStrategies.PreferConsistent();
        ConsumerStrategy consumerStrategy = ConsumerStrategies.Subscribe(topicSet, kafkaParams);

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(jssc, locationStrategy, consumerStrategy);

        JavaDStream<String> lines =
                messages.map(
                        new Function<ConsumerRecord<String, String>, String>() {
                            @Override
                            public String call(ConsumerRecord<String, String> tuple2) throws Exception {
                                return tuple2.value();
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
}
