/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka properties class
 *
 * @since 2022-01-14
 */
public final class KafkaProperties {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProperties.class);

    /**
     * Kafka Topic
     */
    public final static String TOPIC = "kafka-topic";

    private static Properties serverProps = new Properties();

    private static Properties producerProps = new Properties();

    private static Properties consumerProps = new Properties();

    private static Properties clientProps = new Properties();

    private static KafkaProperties instance = null;

    private KafkaProperties() {
        String filePath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main"
                + File.separator + "resources" + File.separator;

        try {
            File proFile = new File(filePath + "producer.properties");

            if (proFile.exists()) {
                producerProps.load(new FileInputStream(filePath + "producer.properties"));
            }

            File conFile = new File(filePath + "producer.properties");

            if (conFile.exists()) {
                consumerProps.load(new FileInputStream(filePath + "consumer.properties"));
            }

            File serFile = new File(filePath + "server.properties");

            if (serFile.exists()) {
                serverProps.load(new FileInputStream(filePath + "server.properties"));
            }

            File cliFile = new File(filePath + "client.properties");

            if (cliFile.exists()) {
                clientProps.load(new FileInputStream(filePath + "client.properties"));
            }
        } catch (IOException e) {
            LOG.info("The Exception occured.", e);
        }
    }

    public synchronized static KafkaProperties getInstance() {
        if (null == instance) {
            instance = new KafkaProperties();
        }

        return instance;
    }

    /**
     * get properties value through key
     * 
     * @param key properties key
     * @param defValue default value
     * @return
     */
    public String getValues(String key, String defValue) {
        String rtValue = null;

        if (null == key) {
            LOG.error("key is null");
        } else {
            rtValue = getPropertiesValue(key);
        }

        if (null == rtValue) {
            LOG.warn("KafkaProperties.getValues return null, key is {}", key);
            rtValue = defValue;
        }

        LOG.info("KafkaProperties.getValues: key is {}; Value is {}", key, rtValue);

        return rtValue;
    }

    private String getPropertiesValue(String key) {
        String rtValue = serverProps.getProperty(key);

        // server.properties中没有，则再向producer.properties中获取
        if (rtValue == null) {
            rtValue = producerProps.getProperty(key);
        }

        // producer中没有，则再向consumer.properties中获取
        if (rtValue == null) {
            rtValue = consumerProps.getProperty(key);
        }

        // consumer没有，则再向client.properties中获取
        if (rtValue == null) {
            rtValue = clientProps.getProperty(key);
        }

        return rtValue;
    }
}
