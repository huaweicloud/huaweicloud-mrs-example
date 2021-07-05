package com.huawei.bigdata.spark.examples;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaStructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
 *   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
 *   comma-separated list of host:port.
 *   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
 *   'subscribePattern'.
 *   |- <assign> Specific TopicPartitions to consume. Json string
 *   |  {"topicA":[0,1],"topicB":[2,4]}.
 *   |- <subscribe> The topic list to subscribe. A comma-separated list of
 *   |  topics.
 *   |- <subscribePattern> The pattern used to subscribe to topic(s).
 *   |  Java regex string.
 *   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
 *   |  specified for Kafka source.
 *   <topics> Different value format depends on the value of 'subscribe-type'.
 */
public class KafkaWordCount {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: KafkaWordCount <bootstrap-servers> " + "<subscribe-type> <topics> <checkpointLocation>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String subscribeType = args[1];
        String topics = args[2];
        String checkpointLocation = args[3];

        SparkSession spark = SparkSession.builder().appName("KafkaWordCount").getOrCreate();
        spark.conf().set("spark.sql.streaming.checkpointLocation", checkpointLocation);

        // Create DataSet representing the stream of input lines from kafka
        Dataset<String> lines =
                spark.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", bootstrapServers)
                        .option(subscribeType, topics)
                        .load()
                        .selectExpr("CAST(value AS STRING)")
                        .as(Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts =
                lines.flatMap(
                                new FlatMapFunction<String, String>() {
                                    @Override
                                    public Iterator<String> call(String x) {
                                        return Arrays.asList(x.split(" ")).iterator();
                                    }
                                },
                                Encoders.STRING())
                        .groupBy("value")
                        .count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

        query.awaitTermination();
    }
}
