package com.huawei.bigdata.spark.examples.streaming;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class StreamingExampleProducer {
  public static void main(String[] args) {
    if (args.length < 2) {
      printUsage();
    }
    String brokerList = args[0];
    String topic = args[1];
    Properties props = new Properties();
    props.put("metadata.broker.list", brokerList);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig conf = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(conf);

    Random random = new Random();

    for (int m = 0; m < Integer.MAX_VALUE / 2; m++) {
      List<KeyedMessage<String, String>> dataForMultipleTopics = new ArrayList<KeyedMessage<String, String>>();
      for (int i = 0; i < 5; i++) {
        dataForMultipleTopics.add(new KeyedMessage<String, String>(topic, String.valueOf(random.nextInt(10))));
      }
      producer.send(dataForMultipleTopics);
      try {
        Thread.sleep(30000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private static void printUsage() {
    System.out.println("Usage: {brokerList} {topic}");
  }
}
