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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Service
public class Consumer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private KafkaConsumer<String, String> consumer;

    // 一次请求的最大等待时间(S)
    private static final int WAIT_TIME = 1;

    private int threadAliveTime = 180000;

    private volatile boolean closed;

    private final CountDownLatch latch;

    /**
     * Consumer constructor
     */
    public Consumer() {
        super("KafkaConsumerExample");
        this.latch = new CountDownLatch(1);
    }

    public void init(KafkaProperties kafkaProperties) {
        Properties properties = new Properties();
        kafkaProperties.initialClientProperties(properties);
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

        this.threadAliveTime = Integer.parseInt(properties.getProperty("consumer.alive.time"));
        this.consumer = new KafkaConsumer<String, String>(properties);
        // 订阅
        this.consumer.subscribe(Collections.singletonList(properties.getProperty("topic")));
    }

    /**
     * 订阅Topic的消息处理函数
     */
    public void run() {
        long recordsCount = 0;
        long startTime = System.currentTimeMillis();
        while (!isTimeout(startTime)) {
            try {
                // 消息消费请求
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofSeconds(WAIT_TIME));
                // 消息处理
                for (ConsumerRecord<String, String> record : records) {
                    LOG.info(String.format("[ConsumerExample], Received message: (%s, %s) at offset %s",
                        record.key(), record.value(), record.offset()));
                }
                recordsCount += records.count();
            } catch (AuthorizationException | UnsupportedVersionException
                     | RecordDeserializationException e) {
                LOG.error(e.getMessage());
                // 无法从异常中恢复
                closeThread();
                latchShutDown();
            } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                LOG.error("Invalid or no offset found, using latest");
                consumer.seekToEnd(e.partitions());
                consumer.commitSync();
            } catch (KafkaException e) {
                LOG.error(e.getMessage());
            }
        }

        LOG.info("Finished consume messages {}", recordsCount);
    }

    public boolean isTimeout(long startTime) {
        long curTime = System.currentTimeMillis();
        if ((curTime - startTime) >= getThreadAliveTime())
            return true;
        return false;
    }

    public int getThreadAliveTime() {
        return this.threadAliveTime;
    }

    public void close() {
        closeThread();
        try {
            this.latch.await();
        } catch (InterruptedException e) {
            LOG.error("consumerThread.latch.await() is error", e);
        }
        this.consumer.close();
    }

    public void closeThread() {
        if (!closed) {
            closed = true;
        }
    }

    public void latchShutDown() {
        latch.countDown();
    }
}
