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

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

@Service
public class Consumer extends ShutdownableThread {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private KafkaConsumer<String, String> consumer;

    // 一次请求的最大等待时间(Ms)
    private static final int WAIT_TIME = 1000;

    private int threadAliveTime = 180000;

    /**
     * Consumer constructor
     */
    public Consumer() {
        super("KafkaConsumerExample", false);
    }

    public void init(KafkaProperties kafkaProperties) {
        Properties props = new Properties();
        kafkaProperties.initialClientProperties(props);
        // 是否自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交offset的时间间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        // 批量消费最大数量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        // 消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        // session超时，超过这个时间consumer没有发送心跳, 触发 rebalance
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 120000);
        // 请求超时s
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        // Key 反序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Value 反序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.threadAliveTime = Integer.parseInt(props.getProperty("consumer.alive.time"));
        this.consumer = new KafkaConsumer<String, String>(props);
        // 订阅
        this.consumer.subscribe(Collections.singletonList(props.getProperty("topic")));
    }

    /**
     * 订阅Topic的消息处理函数
     */
    public void doWork() {
        // 消息消费请求
        ConsumerRecords<String, String> records = this.consumer.poll(WAIT_TIME);
        // 消息处理
        for (ConsumerRecord<String, String> record : records) {
            LOG.info(String.format("[ConsumerExample], Received message: (%s, %s) at offset %s",
                    record.key(), record.value(), record.offset()));
        }

        LOG.info("Finished consume messages {}", records.count());
    }

    public int getThreadAliveTime() {
        return this.threadAliveTime;
    }

    public void close() {
        this.shutdown();
        this.consumer.close();
    }
}
