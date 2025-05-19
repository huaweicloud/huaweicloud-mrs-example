/**
 * Copyright Notice:
 *      Copyright  2013-2024, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */

package com.huawei.bigdata.kafka.example.service;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProperties {
    // Common Client Config
    @Value("${bootstrap.servers:}")
    private String bootstrapServers;

    @Value("${security.protocol:SASL_PLAINTEXT}")
    private String securityProtocol;

    @Value("${sasl.mechanism:PLAIN}")
    private String saslMechanism;

    @Value("${manager_username:}")
    private String username;

    @Value("${manager_password:}")
    private String password;

    @Value("${topic:example-metric1}")
    private String topic;

    @Value("${is.security.mode:true}")
    private boolean isSecurityMode;

    // producer config
    @Value("${isAsync:false}")
    private String isAsync;

    // consumer config
    @Value("${consumer.alive.time:180000}")
    private String consumerAliveTime;

    public KafkaProperties() {
    }

    /**
     * 生产者配置
     */
    @Bean(name = "kafkaProducerTemplate")
    public KafkaTemplate kafkaProducerTemplate() {
        Map<String, Object> props = new HashMap<>();
        this.initPropertiesByResources(props);
        this.initProducerProperties(props);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    /**
     * 消费者配置
     */
    @Bean(name = "KafkaListenerContainerFactory")
    public KafkaListenerContainerFactory integratedEnergyKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory<>();
        Map<String, Object> props = new HashMap<>();
        this.initConsumerProperties(props);
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        // 设置并发数量
        factory.setConcurrency(3);
        return factory;
    }

    // 通过 resources 中的 application.properties 来设置部分参数
    public void initPropertiesByResources(Map<String, Object> properties) {
        // Broker连接地址
        if (isEmpty(this.bootstrapServers)) {
            throw new IllegalArgumentException("The bootstrap.servers is null or empty.");
        }
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);

        // 安全协议类型
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, this.securityProtocol);
        // 安全协议下使用的认证机制
        properties.put(SaslConfigs.SASL_MECHANISM, this.saslMechanism);

        // 动态jaas config
        if (this.isSecurityMode) {
            if (isEmpty(this.username)|| isEmpty(this.password)) {
                throw new IllegalArgumentException("The properties manager_username or manager_password is null or empty.");
            }

            String jaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=%s password=%s;", this.username, this.password);
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        }

        properties.put("topic", this.topic);
        properties.put("isAsync", this.isAsync);
        properties.put("consumer.alive.time", this.consumerAliveTime);
    }

    // 设置生产者的配置
    public void initProducerProperties(Map<String, Object> properties) {
        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // acks=0 把消息发送到kafka就认为发送成功
        // acks=1 把消息发送到kafka leader分区并且写入磁盘就认为发送成功
        // acks=all 把消息发送到kafka leader分区并且leader分区的副本follower对消息进行了同步就任务发送成功
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // KafkaProducer.send() 和 partitionsFor() 方法的最长阻塞时间 ms
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        // 批量处理的最大大小 byte
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 4096);
        // 当生产端积累的消息达到batch-size或接收到消息linger.ms后 producer会将消息发送给kafka
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        // 生产者可用缓冲区的最大值 byte
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 每条消息最大的大小
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);
        // 客户端ID
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "client-1");
        // Key 序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Value 序列化方式
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 消息压缩 none、lz4、gzip、snappy
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
    }

    // 设置消费者的配置
    private Map initConsumerProperties(Map<String, Object> properties) {
        this.initPropertiesByResources(properties);
        // 是否自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交offset的时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        // 批量消费最大数量
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        // session超时，超过这个时间consumer没有发送心跳, 触发 rebalance
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 120000);
        // 请求超时s
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        // Key 反序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Value 反序列化类
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return properties;
    }

    public static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }
}
