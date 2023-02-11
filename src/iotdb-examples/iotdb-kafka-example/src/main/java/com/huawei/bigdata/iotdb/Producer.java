/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.huawei.bigdata.iotdb.security.LoginUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class is a demo to show how to send iotdb data to kafka
 *
 * @since 2022-01-14
 */
public class Producer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    private final KafkaProducer<String, String> producer;

    private final String topic;

    private final Boolean isAsync;

    /**
     * Producer constructor
     *
     * @param topicName kafka topic name
     * @param asyncEnable whether use async
     */
    public Producer(String topicName, Boolean asyncEnable) {
        Properties props = initProperties();
        producer = new KafkaProducer<>(props);
        topic = topicName;
        isAsync = asyncEnable;
    }

    public static Properties initProperties() {
        Properties props = new Properties();
        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        // broker address
        props.put(Constant.BOOTSTRAP_SERVER, kafkaProc.getValues(Constant.BOOTSTRAP_SERVER, "127.0.0.1:21007"));
        // client ID
        props.put(Constant.CLIENT_ID, kafkaProc.getValues(Constant.CLIENT_ID, "DemoProducer"));
        // key serializer
        props.put(Constant.KEY_SERIALIZER,
                kafkaProc.getValues(Constant.KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"));
        // value serializer
        props.put(Constant.VALUE_SERIALIZER, kafkaProc.getValues(Constant.VALUE_SERIALIZER,
                "org.apache.kafka.common.serialization.StringSerializer"));
        // security protocol: support SASL_PLAINTEXT or PLAINTEXT
        props.put(Constant.SECURITY_PROTOCOL, kafkaProc.getValues(Constant.SECURITY_PROTOCOL, "SASL_PLAINTEXT"));
        // service name
        props.put(Constant.SASL_KERBEROS_SERVICE_NAME, "kafka");
        // domain name
        props.put(Constant.KERBEROS_DOMAIN_NAME,
                kafkaProc.getValues(Constant.KERBEROS_DOMAIN_NAME, "hadoop.HADOOP.COM"));

        return props;
    }

    private String[] generateSampleData(int size, long timestamp) {
        String[] data = new String[size];
        float value = 1.0F;
        for (int i = 0; i < size; i++) {
            data[i] = String.format(Constant.IOTDB_DATA_SAMPLE_TEMPLATE, i, timestamp, value);
        }
        return data;
    }

    /**
     * Send messages cyclically.
     */
    public void run() {
        LOG.info("New Producer: start.");
        int messageNo = 1;
        int totalMessage = 10000;

        while (messageNo <= totalMessage) {
            // Construct timeseries message record
            int sensorSize = 100;
            String[] sampleData = generateSampleData(sensorSize, System.currentTimeMillis());
            for (String data : sampleData) {
                long startTime = System.currentTimeMillis();

                String key = String.valueOf(messageNo);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, data);

                if (isAsync) {
                    // use async
                    producer.send(record, new DemoCallBack(startTime, messageNo, data));
                } else {
                    // use sync
                    try {
                        producer.send(record).get();
                    } catch (InterruptedException ie) {
                        LOG.info("The InterruptedException occured.", ie);
                    } catch (ExecutionException ee) {
                        LOG.info("The ExecutionException occured.", ee);
                    }
                }
                messageNo++;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOG.info("The Producer have send {} messages.", messageNo - 1);
        }

    }

    public static void main(String[] args) {
        // whether use security mode
        final boolean isSecurityModel = true;

        if (isSecurityModel) {
            try {
                LOG.info("Security mode start.");

                // !!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                LoginUtil.securityPrepare(Constant.USER_PRINCIPAL, Constant.USER_KEYTAB_FILE);
            } catch (IOException e) {
                LOG.error("Security prepare failure.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        // whether to use the asynchronous sending mode
        final boolean asyncEnable = false;
        Producer producerThread = new Producer(KafkaProperties.TOPIC, asyncEnable);
        producerThread.start();
    }

    class DemoCallBack implements Callback {
        private final Logger logger = LoggerFactory.getLogger(DemoCallBack.class);

        private long startTime;

        private int key;

        private String message;

        public DemoCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * Callback function
         *
         * @param metadata
         * @param exception
         */
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                logger.info("message({}, {}) sent to partition({}), offset({}) in {} ms", key, message,
                        metadata.partition(), metadata.offset(), elapsedTime);
            } else if (exception != null) {
                logger.error("The Exception occured.", exception);
            }
        }
    }
}