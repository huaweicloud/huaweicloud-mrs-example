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

package com.huawei.bigdata.kafka.example.controller;

import com.huawei.bigdata.kafka.example.service.Consumer;
import com.huawei.bigdata.kafka.example.service.KafkaProperties;
import com.huawei.bigdata.kafka.example.service.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    private final static Logger LOG = LoggerFactory.getLogger(MessageController.class);

    @Autowired
    private KafkaProperties kafkaProperties;

    @GetMapping("/produce")
    public String produce() {
        Producer producerThread = new Producer();
        producerThread.init(this.kafkaProperties);
        producerThread.start();
        String message = "Start to produce messages";
        LOG.info(message);
        return message;
    }

    @GetMapping("/consume")
    public String consume() {
        Consumer consumerThread = new Consumer();
        consumerThread.init(this.kafkaProperties);
        consumerThread.start();
        LOG.info("Start to consume messages");

        // 等到180s后将consumer关闭，实际执行过程中可修改
        try {
            Thread.sleep(consumerThread.getThreadAliveTime());
        } catch (InterruptedException e) {
            LOG.info("Occurred InterruptedException: ", e);
        } finally {
            consumerThread.close();
        }

        return "Finished consume messages";
    }
}
