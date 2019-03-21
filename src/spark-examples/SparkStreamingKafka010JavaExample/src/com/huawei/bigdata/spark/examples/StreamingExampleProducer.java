package com.huawei.bigdata.spark.examples;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * product data
 */
public class StreamingExampleProducer {
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      printUsage();
    }
    String brokerList = args[0];
    String topic = args[1];
    String filePath = "/home/data/";
    Properties props = new Properties();
    props.put("metadata.broker.list", brokerList);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig conf = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(conf);

    for (int m = 0; m < Integer.MAX_VALUE / 2; m++) {
      File dir = new File(filePath);
      File[] files = dir.listFiles();
      if (files != null){
        for(File file : files){
          List<KeyedMessage<String, String>> dataForMultipleTopics = new ArrayList<KeyedMessage<String, String>>();
          if(file.isDirectory()){
            System.out.println(file.getName() + "This is a directory!");
          }else{
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader(filePath+file.getName()));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
              //Blank line judgment
              if (!tempString.isEmpty()) {
                dataForMultipleTopics.add(new KeyedMessage<String, String>(topic, tempString));
              }
            }
            // make sure the streams are closed finally.
            reader.close();
          }
          producer.send(dataForMultipleTopics);
        }
      }
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
