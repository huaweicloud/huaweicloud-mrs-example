// scalastyle:off println
package com.huawei.bigdata.spark.examples

import org.apache.spark.sql.SparkSession

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: StructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
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
 *   <protocol>
 *   <service>
 *   <domain>
 */
object SecurityKafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println("Usage: SecurityKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics> <protocol> <service> <domain>")
      System.exit(1)
    }

    val Array(bootstrapServers, subscribeType, topics, protocol, service, domain) = args

    val spark = SparkSession
      .builder
      .appName("SecurityKafkaWordCount")
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .option("kafka.security.protocol", protocol)
      .option("kafka.sasl.kerberos.service.name", service)
      .option("kafka.kerberos.domain.name", domain)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
