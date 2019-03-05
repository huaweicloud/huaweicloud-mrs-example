package com.huawei.bigdata.spark.examples;

import java.io.File;
import java.util.*;

import scala.Tuple2;
import scala.Tuple3;

import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.huawei.hadoop.security.LoginUtil;

/**
 * Consumes messages from one or more topics in Kafka.
 * <batchTime> is the Spark Streaming batch duration in seconds.
 * <topics> is a list of one or more kafka topics to consume from
 * <brokers> is for bootstrapping and the producer will only use it for getting metadata
 */

public class FemaleInfoCollectionPrint {
  public static void main(String[] args) throws Exception {

      Configuration hadoopConf = new Configuration();
      if ("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))) {
          //security mode

          final String userPrincipal = "sparkuser";
          final String USER_KEYTAB_FILE = "user.keytab";
          String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
          String krbFile = filePath + "krb5.conf";
          String userKeyTableFile = filePath + USER_KEYTAB_FILE;

          LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf);
      }

      String checkPointDir = args[0];
      String batchTime = args[1];
      String topics = args[2];
      String brokers = args[3];

      Duration batchDuration = Durations.seconds(Integer.parseInt(batchTime));


      // Create a Streaming startup environment.
      SparkConf conf = new SparkConf().setAppName("DataSightStreamingExample");
      JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);

      //Configure the CheckPoint directory for the Streaming.
      jssc.checkpoint(checkPointDir);

      // Get the list of topic used by kafka
      HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
      HashMap<String, String> kafkaParams = new HashMap<String, String>();
      kafkaParams.put("metadata.broker.list", brokers);

      // Create direct kafka stream with brokers and topics
      // Receive data from the Kafka and generate the corresponding DStream
      JavaDStream<String> lines = KafkaUtils
              .createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
                      topicsSet).map(new Function<Tuple2<String, String>, String>() {
                  public String call(Tuple2<String, String> tuple2) {
                      return tuple2._2();
                  }
              });

      // Obtain field properties in each row.
      JavaDStream<Tuple3<String, String, Integer>> records =
              lines.map(new Function<String, Tuple3<String, String, Integer>>() {
                  public Tuple3<String, String, Integer> call(String line) throws Exception {
                      String[] elems = line.split(",");
                      return new Tuple3<String, String, Integer>(elems[0], elems[1], Integer.parseInt(elems[2]));
                  }
              });

      // Filter data about the time that female netizens spend online
      JavaDStream<Tuple2<String, Integer>> femaleRecords =
              records.filter(new Function<Tuple3<String, String, Integer>, Boolean>() {
                  public Boolean call(Tuple3<String, String, Integer> line) throws Exception {
                      if (line._2().equals("female")) {
                          return true;
                      } else {
                          return false;
                      }
                  }
              }).map(new Function<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
                  public Tuple2<String, Integer> call(Tuple3<String, String, Integer> stringStringIntegerTuple3)
                          throws Exception {
                      return new Tuple2<String, Integer>(stringStringIntegerTuple3._1(), stringStringIntegerTuple3._3());
                  }
              });

      // Filter data about users whose consecutive online duration exceeds the threshold.
      JavaDStream<Tuple2<String, Integer>> upTimeUser =
              femaleRecords.filter(new Function<Tuple2<String, Integer>, Boolean>() {
                  public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                      if (stringIntegerTuple2._2() > 30) {
                          return true;
                      } else {
                          return false;
                      }
                  }
              });

      // print the results
      upTimeUser.print();

      //The Streaming system starts.
      jssc.start();
      jssc.awaitTermination();
  }
}
