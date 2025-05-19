package com.huawei.bigdata.kafka.example.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumerService.class);

  @Value("${topic:example-metric1}")
  private String topic;

  @KafkaListener(
    containerFactory = "KafkaListenerContainerFactory",
    id = "id",
    idIsGroup = false,
    groupId = "groupid",
    topics = "${topic}"
  )
  public void listen(ConsumerRecord<?, ?> record) {
    LOG.info("The consumer poll 1 record from kafka, the topic is {}, the partition is {}, the offset is {}, the " +
      "key is {}, the value is {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
  }
}
