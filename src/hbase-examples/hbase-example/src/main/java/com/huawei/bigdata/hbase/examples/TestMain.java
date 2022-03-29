/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import com.huawei.hadoop.security.LoginUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Function description:
 *
 * hbase-example test main class
 *
 * @since 2013
 */

public class TestMain {
    private static final Logger LOG = LoggerFactory.getLogger(TestMain.class.getName());
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static Configuration conf = null;
    private static String krb5File = null;
    private static String userName = null;
    private static String userKeytabFile = null;

    public static void main(String[] args) {
        try {
            init();
            login();
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }

        // test hbase
        HBaseSample oneSample;
        try {
            oneSample = new HBaseSample(conf);
            oneSample.test();
        } catch (IOException e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish HBase -------------------");

        // test phoenix
        PhoenixSample anotherSample;
        try {
            anotherSample = new PhoenixSample(conf);
            anotherSample.test();
        } catch (SQLException e) {
            LOG.error("Failed to test Phoenix because ", e);
        }
        LOG.info("-----------finish Phoenix -------------------");
    }

    private static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            userName = "hbaseuser1";
            //In Windows environment
            String userdir = TestMain.class.getClassLoader().getResource("conf").getPath() + File.separator;
            //In Linux environment
            //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

            userKeytabFile = userdir + "user.keytab";
            krb5File = userdir + "krb5.conf";
            /*
             * if need to connect zk, please provide jaas info about zk. of course,
             * you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath +
             * "jaas.conf"); but the demo can help you more : Note: if this process
             * will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    private static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        //In Windows environment
        String userdir = TestMain.class.getClassLoader().getResource("conf").getPath() + File.separator;
        //In Linux environment
        //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        conf.addResource(new Path(userdir + "core-site.xml"), false);
        conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
        conf.addResource(new Path(userdir + "hbase-site.xml"), false);
    }
}
