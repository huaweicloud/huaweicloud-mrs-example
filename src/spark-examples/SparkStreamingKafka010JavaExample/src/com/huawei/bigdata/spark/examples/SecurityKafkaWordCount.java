package com.huawei.bigdata.spark.examples;

import java.io.File;
import java.util.*;
import scala.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import com.huawei.hadoop.security.LoginUtil;

/**
 * Consumes messages from one or more topics in Kafka.
 * <checkPointDir> is the Spark Streaming checkpoint directory.
 * <brokers> is for bootstrapping and the producer will only use it for getting metadata
 * <topics> is a list of one or more kafka topics to consume from
 * <batchTime> is the Spark Streaming batch duration in seconds.
 */
public class SecurityKafkaWordCount
{
  public static void main(String[] args) throws Exception {
    JavaStreamingContext ssc = createContext(args);

    //The Streaming system starts.
    ssc.start();
    try {
      ssc.awaitTermination();
    } catch (InterruptedException e) {
    }
  }

  private static JavaStreamingContext createContext(String[] args) throws Exception {
    Configuration hadoopConf = new Configuration();
    if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))){
      //security mode

      final String userPrincipal = "sparkuser";
      final String USER_KEYTAB_FILE = "user.keytab";
      String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
      String krbFile = filePath + "krb5.conf";
      String userKeyTableFile = filePath + USER_KEYTAB_FILE;

      LoginUtil.setJaasFile(userPrincipal, userKeyTableFile);
      LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf);
    }

    String checkPointDir = args[0];
    String brokers = args[1];
    String topics = args[2];
    String batchSize = args[3];

    // Create a Streaming startup environment.
    SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(Long.parseLong(batchSize) * 1000));

    //Configure the CheckPoint directory for the Streaming.
    //This parameter is mandatory because of existence of the window concept.
	ssc.checkpoint(checkPointDir);

    // Get the list of topic used by kafka
    String[] topicArr = topics.split(",");
    Set<String> topicSet = new HashSet<String>(Arrays.asList(topicArr));
    Map<String, Object> kafkaParams = new HashMap();
    kafkaParams.put("bootstrap.servers", brokers);
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaParams.put("group.id", "DemoConsumer");
    kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
    kafkaParams.put("sasl.kerberos.service.name", "kafka");
    kafkaParams.put("kerberos.domain.name", "hadoop.hadoop.com");

    LocationStrategy locationStrategy = LocationStrategies.PreferConsistent();
    ConsumerStrategy consumerStrategy = ConsumerStrategies.Subscribe(topicSet, kafkaParams);

    // Create direct kafka stream with brokers and topics
    // Receive data from the Kafka and generate the corresponding DStream
    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy);

    // Obtain field properties in each row.
    JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String, String>, String>() {
      @Override
      public String call(ConsumerRecord<String, String> tuple2) throws Exception {
        return tuple2.value();
      }
    });

    // Aggregate the total time that calculate word count
    JavaPairDStream<String, Integer> wordCounts = lines.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
          }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    }).updateStateByKey(
        new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
          @Override
          public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
            int out = 0;
            if (state.isPresent()) {
              out += state.get();
            }
            for (Integer v : values) {
              out += v;
            }
            return Optional.of(out);
          }
        });

    // print the results
    wordCounts.print();
    return ssc;
  }
}
