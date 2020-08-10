package com.huawei.bigdata.spark.examples.streaming;

import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class StreamingExampleProducer {
    private static final int TOTAL_USERS = 10;
    private static final int USERS_EVERY_30_SECONDS = 5;
    private static final int MAX_FEE = 100;

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
        }
        String brokers = args[0];
        String topic = args[1];

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();
        for (int m = 0; m < Integer.MAX_VALUE / 2; m++) {
            Set<Integer> set = new HashSet<>();
            for (int i = 0; i < USERS_EVERY_30_SECONDS; i++) {
                // one record: userId,fee
                String recordString = String.join(",",
                        String.valueOf(randomUser(set, random) + 1),
                        String.valueOf(random.nextFloat() * MAX_FEE));
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, recordString);
                producer.send(record);
                System.out.println("Record sent: " + recordString);
            }
            set.clear();

            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static int randomUser(Set<Integer> usersSet, Random random) {
        while (true) {
            int newUser = random.nextInt(StreamingExampleProducer.TOTAL_USERS);
            if (!usersSet.contains(newUser)) {
                usersSet.add(newUser);
                return newUser;
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage: StreamingExampleProducer <brokerList> <topic>");
    }
}
