/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package com.huawei.bigdata.hudi.examples;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 功能描述
 * 向指定kafka的topic发送数据
 *
 * @since 2021-03-17
 */
public class ProducerDemo {
    public static void main(String[] args) throws IOException, InterruptedException {
        String kafkaURL = args[0];
        String topicName = args[1];
        String kerberosDomainName = args[2];
        String securityProtocol = args[3];
        String kerberosServiceName = args[4];
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", kafkaURL);
        kafkaProps.put("kerberos.domain.name", kerberosDomainName);
        kafkaProps.put("security.protocol", securityProtocol);
        kafkaProps.put("sasl.kerberos.service.name", kerberosServiceName);
        kafkaProps.put("acks", "1");
        kafkaProps.put("retries", 0);
        kafkaProps.put("compression.type", "snappy");
        kafkaProps.put("batch.size", 100);
        kafkaProps.put("linger.ms", 1);
        kafkaProps.put("buffer.memory", 1048576);
        kafkaProps.put("max.in.flight.requests.per.connection", 1);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
        Random random = new Random(10000);
        for (int i = 0; i < 100; i++) {
            producer
                .send(new ProducerRecord<String, String>(topicName, "{\"age\":\"" + random.nextInt() + "\",\"id\":\""
                    + i + "\",\"job\":\"" + random.nextInt(5) + "\",\"name\":\"" + random.nextInt() + "\"}"));
        }
        producer.close();
    }
}
