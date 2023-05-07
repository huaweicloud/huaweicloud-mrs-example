/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.hbase.example.springboot.client.service;

import com.huawei.fusioninsight.hbase.example.springboot.client.controller.HBaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Order(value = 1)
public class QuartzTask implements ApplicationRunner {
    private static final Logger LOG = LoggerFactory.getLogger(QuartzTask.class);

    @Autowired
    private PropertyConfig propertyConfig;

    @Override
    public void run(ApplicationArguments args) {
        LOG.info("QuartzTask start......");

        LOG.info("keytabPath : " + propertyConfig.getUserKeytabFile());
        LOG.info("krb5confPath : " + propertyConfig.getKrb5File());
        LOG.info("confPath : " + propertyConfig.getConfDir());

        HBaseController.principal = propertyConfig.getPrincipal();
        HBaseController.userKeytabFile = propertyConfig.getUserKeytabFile();
        HBaseController.krb5File = propertyConfig.getKrb5File();
        HBaseController.confDir = propertyConfig.getConfDir();
        HBaseController.zookeeperPrincipal = propertyConfig.getZookeeperPrincipal();

        try {
            HBaseController.init();
            HBaseController.login();
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
        }
    }

}
