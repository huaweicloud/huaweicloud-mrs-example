/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.hbase.example.springboot.client.controller;

import com.huawei.fusioninsight.hbase.example.springboot.client.security.LoginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * HBase springboot样例controller
 *
 * @since 2022
 */
@RestController
@RequestMapping("/hbase")
public class HBaseController {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseController.class);

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static final String HBASE_END = "-----------finish HBase -------------------";

    private static final String PHOENIX_END = "-----------finish Phoenix -------------------";

    private static Configuration conf = null;

    public static String principal;

    public static String userKeytabFile;

    public static String krb5File;

    public static String confDir;

    public static String zookeeperPrincipal;

    @GetMapping("HBaseDemo")
    public String HBaseDemo() {
        // test hbase
        HBaseSample oneSample;
        String msg = "";
        try {
            oneSample = new HBaseSample(conf);
            oneSample.test();
        } catch (IOException e) {
            LOG.error("Failed to test HBase because ", e);
            msg = "Failed to test HBase.";
        }
        LOG.info(HBASE_END);
        return msg.isEmpty() ? HBASE_END : msg;
    }

    @GetMapping("PhoenixDemo")
    public String PhoenixDemo() {
        // test phoenix
        PhoenixSample anotherSample;
        String msg = "";
        try {
            anotherSample = new PhoenixSample(conf);
            anotherSample.test();
        } catch (SQLException e) {
            LOG.error("Failed to test Phoenix because ", e);
            msg = "Failed to test Phoenix.";
        }
        LOG.info(PHOENIX_END);
        return msg.isEmpty() ? PHOENIX_END : msg;
    }

    public static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            /*
             * if need to connect zk, please provide jaas info about zk. of course,
             * you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath +
             * "jaas.conf"); but the demo can help you more : Note: if this process
             * will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, zookeeperPrincipal);
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, principal, userKeytabFile);
            LoginUtil.login(principal, userKeytabFile, krb5File, conf);
        }
    }

    public static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        conf.addResource(new Path(confDir + File.separator + "core-site.xml"), false);
        conf.addResource(new Path(confDir + File.separator + "hdfs-site.xml"), false);
        conf.addResource(new Path(confDir + File.separator + "hbase-site.xml"), false);
        // 启用keytab renewal
        conf.set("hadoop.kerberos.keytab.login.autorenewal.enabled", "true");
    }

}
