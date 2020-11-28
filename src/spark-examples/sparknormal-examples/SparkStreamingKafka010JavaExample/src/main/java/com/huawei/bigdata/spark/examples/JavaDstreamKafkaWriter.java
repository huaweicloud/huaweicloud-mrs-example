package com.huawei.bigdata.spark.examples;

import com.huawei.spark.streaming.kafka010.JavaDStreamKafkaWriterFactory;

import scala.collection.JavaConverters;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.*;

/**
 * Exaple code to demonstrate the usage of dstream.writeToKafka API
 *
 * Parameter description:
 * <groupId> is the group ID for the consumer.
 * <brokers> is for bootstrapping and the producer will only use
 * <topic> is a kafka topic to consume from.
 */
public class JavaDstreamKafkaWriter {
    public static void main(String[] args) throws InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: JavaDstreamKafkaWriter <groupId> <brokers> <topic>");
            System.exit(1);
        }

        final String groupId = args[0];
        final String brokers = args[1];
        final String topic = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("KafkaWriter");

        // Populate Kafka properties
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "smallest");

        // Create Spark Java streaming context
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(500));

        // Populate data to write to kafka
        List<String> sentData = new ArrayList();
        sentData.add("kafka_writer_test_msg_01");
        sentData.add("kafka_writer_test_msg_02");
        sentData.add("kafka_writer_test_msg_03");

        // Create Java RDD queue
        Queue<JavaRDD<String>> sent = new LinkedList();
        sent.add(ssc.sparkContext().parallelize(sentData));

        // Create java Dstream with the data to be written
        JavaDStream wStream = ssc.queueStream(sent);

        // Write to kafka
        JavaDStreamKafkaWriterFactory.fromJavaDStream(wStream)
                .writeToKafka(
                        JavaConverters.mapAsScalaMapConverter(kafkaParams).asScala(),
                        new Function<String, ProducerRecord<String, byte[]>>() {
                            @Override
                            public ProducerRecord<String, byte[]> call(String s) throws Exception {
                                return new ProducerRecord(topic, s.toString().getBytes());
                            }
                        });

        ssc.start();
        ssc.awaitTermination();
    }
}
