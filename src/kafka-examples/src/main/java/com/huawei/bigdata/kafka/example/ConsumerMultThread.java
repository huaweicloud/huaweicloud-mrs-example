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

import com.huawei.bigdata.kafka.example.security.LoginUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;


/**
 * 消费者类
 */
public class ConsumerMultThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerMultThread.class);

    /**
     * 用户自己申请的机机账号keytab文件名称
     */
    private static final String USER_KEYTAB_FILE = "用户自己申请的机机账号keytab文件名称";

    /**
     * 用户自己申请的机机账号名称
     */
    private static final String USER_PRINCIPAL = "用户自己申请的机机账号名称";

    /**
     * 并发的线程数
     */
    private static final int CONCURRENCY_THREAD_NUM = 3;

    /**
     * 待消费的topic
     */
    private String topic;

    /**
     * 拉取数据时间间隔
     */
    private int waitTime = 1000;

    public ConsumerMultThread(String topic) {
        this.topic = topic;
    }

    public void run() {
        LOG.info("Consumer: start.");
        Properties props = Consumer.initProperties();

        for (int threadNum = 0; threadNum < CONCURRENCY_THREAD_NUM; threadNum++) {
            new ConsumerThread(threadNum, topic, props).start();
            LOG.info("Consumer Thread " + threadNum + " Start.");
        }
    }

    public static void main(String[] args) {
        // 安全模式下启用
        if (LoginUtil.isSecurityModel()) {
            try {
                LOG.info("Securitymode start.");

                //!!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                LoginUtil.securityPrepare(USER_PRINCIPAL, USER_KEYTAB_FILE);
            } catch (IOException e) {
                LOG.error("Security prepare failure.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        // 启动消费线程，其中KafkaProperties.topic为待消费的topic名称
        ConsumerMultThread consumerThread = new ConsumerMultThread(KafkaProperties.TOPIC);
        consumerThread.start();
    }

    /**
     * 消费者线程类
     */
    private class ConsumerThread extends Thread {
        private int threadNum = 0;
        private String topic;
        private Properties props;
        private KafkaConsumer<String, String> consumer = null;

        /**
         * 消费者线程类构造方法
         *
         * @param threadNum 线程号
         * @param topic     topic
         */
        public ConsumerThread(int threadNum, String topic, Properties props) {
            super("ConsumerThread" + threadNum);
            this.threadNum = threadNum;
            this.topic = topic;
            this.props = props;
            this.consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Collections.singleton(this.topic));
        }

        public void run() {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(waitTime);
                for (ConsumerRecord<String, String> record : records) {
                    LOG.info("Consumer Thread-" + this.threadNum + " partitions:" + record.partition() + " record: "
                        + record.value() + " offsets: " + record.offset());
                }
            }
        }
    }
}
