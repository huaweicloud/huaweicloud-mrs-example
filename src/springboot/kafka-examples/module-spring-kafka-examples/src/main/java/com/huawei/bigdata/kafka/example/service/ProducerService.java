package com.huawei.bigdata.kafka.example.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;

@Service
public class ProducerService {
  private static final Logger LOG = LoggerFactory.getLogger(ProducerService.class);

  // KafkaTemplate 是 Spring Kafka 提供的一个高级抽象，用于简化与 Kafka 的交互。
  @Autowired
  private final KafkaTemplate<String, Object> kafkaProducerTemplate;

  // 是否使用异步方式发送 Kafka 消息
  @Value("${isAsync}")
  private Boolean isAsync;

  public ProducerService(KafkaTemplate<String, Object> kafkaProducerTemplate) {
    this.kafkaProducerTemplate = kafkaProducerTemplate;
    LOG.info("isAsync: {}", isAsync);
  }

  public void sendMessage(String topic, Object o) {
    // 使用异步方式发送 Kafka 消息
    if (isAsync) {
      // 分区设置为 null，交给 kafka 自己去分配
      ProducerRecord<String, Object> record = new ProducerRecord<>(
              topic, null, System.currentTimeMillis(), String.valueOf(o.hashCode()), o
      );

      ListenableFuture<SendResult<String, Object>> future = kafkaProducerTemplate.send(record);

      future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
        @Override
        public void onSuccess(SendResult<String, Object> result) {
          LOG.info("The producer send message to topic {} successfully",
              result.getRecordMetadata().topic());
        }

        @Override
        public void onFailure(Throwable ex) {
          LOG.error("The producer send async message failed, because {}", ex.getMessage());
        }
      });
    } else {
    // 使用同步方式发送 Kafka 消息
      try {
        SendResult<String, Object> sendResult = kafkaProducerTemplate.send(topic, o).get();
        if (sendResult.getRecordMetadata() != null) {
          LOG.info("The producer send message {} to topic {} successfully",
            sendResult.getProducerRecord().value().toString(), sendResult.getProducerRecord().topic());
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("The producer send message failed, because {}", e.getMessage());
      }
    }
  }
}
