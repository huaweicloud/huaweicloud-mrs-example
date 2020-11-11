package com.huawei.bigdata.spark.examples.streaming;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.io.IOException;
import java.util.*;

/**
 * run streaming task and select table1 data from hbase, then update to table1
 */
public class SparkOnStreamingToHbase {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            printUsage();
        }

        String checkPointDir = args[0];
        String topics = args[1];
        final String brokers = args[2];

        Duration batchDuration = Durations.seconds(5);
        SparkConf sparkConf = new SparkConf().setAppName("SparkOnStreamingToHbase");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, batchDuration);

        // set CheckPoint dir
        if (!"nocp".equals(checkPointDir)) {
            jssc.checkpoint(checkPointDir);
        }

        final String columnFamily = "cf";
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "testGroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        kafkaParams.put("enable.auto.commit", true);

        String[] topicArr = topics.split(",");
        Set<String> topicSet = new HashSet<String>(Arrays.asList(topicArr));

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams));
        JavaDStream<String> lines =
                stream.map(
                        new Function<ConsumerRecord<String, String>, String>() {
                            public String call(ConsumerRecord<String, String> v1) throws Exception {
                                return v1.value();
                            }
                        });

        lines.foreachRDD(
                new VoidFunction<JavaRDD<String>>() {
                    public void call(JavaRDD<String> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<String>>() {
                                    public void call(Iterator<String> iterator) throws Exception {
                                        hBaseWriter(iterator, columnFamily);
                                    }
                                });
                    }
                });

        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * do write hbase in executor
     *
     * @param iterator     message
     * @param columnFamily columnFamily
     */
    private static void hBaseWriter(Iterator<String> iterator, String columnFamily) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("table1"));
            System.out.println("connect sucessfully");
            List<Get> rowList = new ArrayList<Get>();
            while (iterator.hasNext()) {
                Get get = new Get(iterator.next().getBytes());
                rowList.add(get);
            }
            // get data from table1
            Result[] resultDataBuffer = table.get(rowList);

            // set data for table1
            List<Put> putList = new ArrayList<Put>();
            for (int i = 0; i < resultDataBuffer.length; i++) {
                String row = new String(rowList.get(i).getRow());
                Result resultData = resultDataBuffer[i];
                if (!resultData.isEmpty()) {
                    // get value by column Family and colomn qualifier
                    String aCid = Bytes.toString(resultData.getValue(columnFamily.getBytes(), "cid".getBytes()));
                    Put put = new Put(Bytes.toBytes(row));

                    // calculate result value
                    int resultValue = Integer.valueOf(row) + Integer.valueOf(aCid);
                    put.addColumn(
                            Bytes.toBytes(columnFamily),
                            Bytes.toBytes("cid"),
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

    private static void printUsage() {
        System.out.println("Usage: {checkPointDir} {topic} {brokerList}");
        System.exit(1);
    }
}
