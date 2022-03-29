/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import com.huawei.bigdata.iotdb.security.LoginUtil;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Multiple Consumer Thread
 *
 * @since 2022-01-14
 */
public class KafkaConsumerMultThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerMultThread.class);

    private static final int CONCURRENCY_THREAD_NUM = 3;

    private String topic;

    private IoTDBSessionPool sessionPool;

    private int waitTime = 1000;

    public KafkaConsumerMultThread(String topic, IoTDBSessionPool sessionPool) {
        this.topic = topic;
        this.sessionPool = sessionPool;
    }

    public void run() {
        LOG.info("Consumer: start.");
        Properties props = initProperties();

        for (int threadNum = 0; threadNum < CONCURRENCY_THREAD_NUM; threadNum++) {
            new ConsumerThread(threadNum, topic, props, sessionPool).start();
            LOG.info("Consumer Thread {} Start.", threadNum);
        }
    }

    public static Properties initProperties() {
        Properties props = new Properties();
        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        // broker address
        props.put(Constant.BOOTSTRAP_SERVER, kafkaProc.getValues(Constant.BOOTSTRAP_SERVER, "127.0.0.1:21007"));
        // group id
        props.put(Constant.GROUP_ID, kafkaProc.getValues(Constant.GROUP_ID, "DemoConsumer1"));
        // enable auto commit offset
        props.put(Constant.ENABLE_AUTO_COMMIT, kafkaProc.getValues(Constant.ENABLE_AUTO_COMMIT, "true"));
        // auto commit offset interval time
        props.put(Constant.AUTO_COMMIT_INTERVAL_MS, kafkaProc.getValues(Constant.AUTO_COMMIT_INTERVAL_MS, "1000"));
        // session timeout
        props.put(Constant.SESSION_TIMEOUT_MS, kafkaProc.getValues(Constant.SESSION_TIMEOUT_MS, "30000"));
        // key deserializer
        props.put(Constant.KEY_DESERIALIZER, kafkaProc.getValues(Constant.KEY_DESERIALIZER,
                "org.apache.kafka.common.serialization.StringDeserializer"));
        // value deserializer
        props.put(Constant.VALUE_DESERIALIZER, kafkaProc.getValues(Constant.VALUE_DESERIALIZER,
                "org.apache.kafka.common.serialization.StringDeserializer"));
        // Security protocol: support SASL_PLAINTEXT or PLAINTEXT
        props.put(Constant.SECURITY_PROTOCOL, kafkaProc.getValues(Constant.SECURITY_PROTOCOL, "SASL_PLAINTEXT"));
        // service name
        props.put(Constant.SASL_KERBEROS_SERVICE_NAME, "kafka");
        // domain name
        props.put(Constant.KERBEROS_DOMAIN_NAME,
                kafkaProc.getValues(Constant.KERBEROS_DOMAIN_NAME, "hadoop.HADOOP.COM"));

        return props;
    }

    public static void main(String[] args) {
        // whether use security mode
        final boolean isSecurityModel = true;

        if (isSecurityModel) {
            try {
                LOG.info("Securitymode start.");

                // !!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                LoginUtil.securityPrepare(Constant.USER_PRINCIPAL, Constant.USER_KEYTAB_FILE);
            } catch (IOException e) {
                LOG.error("Security prepare failure.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        // create IoTDB seesion connection pool
        IoTDBSessionPool sessionPool = new IoTDBSessionPool("127.0.0.1", 22260, "root", "root", 3);

        // start consumer thread
        KafkaConsumerMultThread consumerThread = new KafkaConsumerMultThread(KafkaProperties.TOPIC, sessionPool);
        consumerThread.start();
    }

    /**
     * consumer thread
     */
    private class ConsumerThread extends ShutdownableThread {
        private int threadNum;
        private String topic;
        private KafkaConsumer<String, String> consumer;
        private IoTDBSessionPool sessionPool;

        public ConsumerThread(int threadNum, String topic, Properties props, IoTDBSessionPool sessionPool) {
            super("ConsumerThread" + threadNum, true);
            this.threadNum = threadNum;
            this.topic = topic;
            this.sessionPool = sessionPool;
            this.consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singleton(this.topic));
        }

        public void doWork() {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(waitTime));
            for (ConsumerRecord<String, String> record : records) {
                LOG.info("Consumer Thread-{} partitions:{} record:{} offsets:{}", this.threadNum, record.partition(),
                        record.value(), record.offset());

                // insert kafka consumer data to iotdb
                sessionPool.insertRecord(record.value());
            }
        }
    }
}