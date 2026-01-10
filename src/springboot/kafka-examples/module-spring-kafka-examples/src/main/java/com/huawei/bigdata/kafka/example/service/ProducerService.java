package com.huawei.bigdata.kafka.example.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import jakarta.annotation.PostConstruct;

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
    }

    @PostConstruct
    public void init() {
        LOG.info("isAsync: {}", isAsync);
    }

    public void sendMessage(String topic, Object o) {
        // 使用异步方式发送 Kafka 消息
        if (isAsync) {
            // 分区设置为 null，交给 kafka 自己去分配
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                topic, null, System.currentTimeMillis(), String.valueOf(o.hashCode()), o
            );

            // 直接使用 CompletableFuture
            CompletableFuture<SendResult<String, Object>> future = kafkaProducerTemplate.send(record);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    LOG.info("The producer send message to topic {} successfully",
                        result.getRecordMetadata().topic());
                } else {
                    LOG.error("The producer send message failed, because {}", ex.getMessage());
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
