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

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class Producer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    private KafkaProducer<String, String> producer;
    private String topic;
    private Boolean isAsync;

    // 默认发送100条消息
    private static final int MESSAGE_NUM = 100;

    /**
     * Producer constructor
     */
    public Producer() {
    }

    public void init(KafkaProperties kafkaProperties) {
        Properties properties = new Properties();
        kafkaProperties.initialClientProperties(properties);
        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // acks=0 把消息发送到kafka就认为发送成功
        // acks=1 把消息发送到kafka leader分区并且写入磁盘就认为发送成功
        // acks=all 把消息发送到kafka leader分区并且leader分区的副本follower对消息进行了同步就任务发送成功
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // KafkaProducer.send() 和 partitionsFor() 方法的最长阻塞时间 ms
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 6000);
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
        // 自定义分区
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SimplePartitioner.class.getName());

        this.producer = new KafkaProducer<String, String>(properties);
        this.topic = properties.getProperty("topic");
        // 是否使用异步发送模式
        this.isAsync = Boolean.getBoolean(properties.getProperty("isAsync"));
    }

    /**
     * 生产者线程执行函数，循环发送消息。
     */
    public void run() {
        LOG.info("New Producer: start.");
        int messageNo = 1;
        // 指定发送多少条消息后sleep1秒
        int intervalMessages = 10;
        
        while (messageNo <= MESSAGE_NUM) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            
            // 构造消息记录
            String key = String.valueOf(messageNo);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, key, messageStr);
            
            if (this.isAsync) {
                // 异步发送
                this.producer.send(record, new DemoCallBack(startTime, messageNo, messageStr));
            } else {
                try {
                    // 同步发送
                    this.producer.send(record).get();
                } catch (InterruptedException ie) {
                    LOG.error("Occurred InterruptedException: ", ie);
                } catch (ExecutionException ee) {
                    LOG.error("Occurred ExecutionException: ", ee);
                }
            }

            messageNo++;
            
            if (messageNo % intervalMessages == 0) {
                // 每发送intervalMessage条消息sleep1秒
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                LOG.info("The Producer have send {} messages.", messageNo);
            }
        }

        LOG.info(String.format("Finished send %s messages", MESSAGE_NUM));
    }

    class DemoCallBack implements Callback {
        private final Logger LOGGER = LoggerFactory.getLogger(DemoCallBack.class);

        private long startTime;
        private int key;
        private String message;

        public DemoCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * 回调函数，用于处理异步发送模式下，消息发送到服务端后的处理。
         *
         * @param metadata  元数据信息
         * @param exception 发送异常。如果没有错误发生则为Null。
         */
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - this.startTime;
            if (metadata != null) {
                LOGGER.info(String.format("message(%s, %s) sent to partition(%s), offset(%s) in %s ms",
                        this.key, this.message, metadata.partition(), metadata.offset(), elapsedTime));
            } else if (exception != null) {
                LOGGER.error("Occurred exception: ", exception);
            }
        }
    }
}