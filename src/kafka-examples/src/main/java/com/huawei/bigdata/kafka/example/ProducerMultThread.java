/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.bigdata.kafka.example;

import java.io.IOException;
import java.util.Properties;

import com.huawei.bigdata.kafka.example.security.LoginUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMultThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerMultThread.class);

    /**
     * 并发的线程数
     */
    private static final int PRODUCER_THREAD_COUNT = 3;

    /**
     * 发送的topic名
     */
    private final String topic;

    /**
     * 用户自己申请的机机账号keytab文件名称
     */
    private static final String USER_KEYTAB_FILE = "请修改为真实keytab文件名";

    /**
     * 用户自己申请的机机账号名称
     */
    private static final String USER_PRINCIPAL = "请修改为真实用户名称";

    /**
     * Thread Safe KafkaProducer
     */
    private final KafkaProducer<String, String> producer;

    public ProducerMultThread(String produceToTopic) {
        this.topic = produceToTopic;

        // 创建生产者对象
        Properties props = Producer.initProperties();
        this.producer = new KafkaProducer<String, String>(props);
    }

    /**
     * 启动多个线程进行发送
     */
    public void run() {
        // 指定的线程号，仅用于区分不同的线程
        for (int threadNum = 0; threadNum < PRODUCER_THREAD_COUNT; threadNum++) {
            ProducerThread producerThread = new ProducerThread(threadNum);
            producerThread.start();
        }
    }

    public static void main(String[] args) {
        if (LoginUtil.isSecurityModel()) {
            try {
                LOG.info("Securitymode start.");

                // !!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                LoginUtil.securityPrepare(USER_PRINCIPAL, USER_KEYTAB_FILE);
            } catch (IOException e) {
                LOG.error("Security prepare failure.");
                LOG.error("The IOException occured.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        ProducerMultThread producerMultThread = new ProducerMultThread(KafkaProperties.TOPIC);
        producerMultThread.start();
    }

    /**
     * 生产者线程类
     */
    private class ProducerThread extends Thread {
        private int sendThreadId;

        /**
         * 生产者线程类构造方法
         *
         * @param threadNum 线程号
         */
        public ProducerThread(int threadNum) {
            this.sendThreadId = threadNum;
        }

        public void run() {
            LOG.info("Producer: start.");

            // 用于记录消息条数
            int messageCount = 1;

            // 每个线程发送的消息条数
            int messagesPerThread = 5000;
            while (messageCount <= messagesPerThread) {

                // 待发送的消息内容
                String messageStr = new String("Message_" + sendThreadId + "_" + messageCount);

                // 此处对于同一线程指定相同Key值，确保每个线程只向同一个Partition生产消息
                String key = String.valueOf(sendThreadId);

                // 消息发送
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, messageStr);
                producer.send(record);
                LOG.info("Producer: send " + messageStr + " to " + topic + " with key: " + key);
                messageCount++;

                // 每隔1s，发送1条消息
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            try {
                producer.close();
                LOG.info("Producer " + this.sendThreadId + " closed.");
            } catch (Throwable e) {
                LOG.error("Error when closing producer", e);
            }
        }
    }
}