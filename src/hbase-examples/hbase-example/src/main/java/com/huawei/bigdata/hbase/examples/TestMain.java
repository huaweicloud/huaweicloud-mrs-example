/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2023. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import com.huawei.hadoop.security.LoginUtil;
import com.huawei.hadoop.security.Utils;

import org.apache.hadoop.conf.Configuration;
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

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    /** need to change the value based on the cluster information */
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.YourDomainName";

    private static final String USER_KEYTAB_FILE = "user.keytab";

    private static final String KRB5_CONF_FILE = "krb5.conf";

    private static final String USER_NAME = "hbaseuser";

    public static void main(String[] args) {
        try {
            login();
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }
        LOG.info("-----------begin Test -------------------");
        testHBaseSample();
        testMultiThreadSample();
        // By default, dual read test is skip, you can test it when needed
        // testHBaseDualReadSample();
        // Test Global Secondary Index, if no need, you can skip it.
        testGlobalSecondaryIndexSample();
        testPhoenixSample();
    }

    private static void testHBaseSample() {
        try {
            HBaseSample hBaseSample = new HBaseSample();
            hBaseSample.createConnection();
            hBaseSample.test();
        } catch (IOException e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish HBase Sample-------------------");
    }

    private static void testMultiThreadSample() {
        try {
            MultiThreadSample multiThreadSample = new MultiThreadSample();
            multiThreadSample.test();
        } catch (IOException e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish MultiThread Sample -------------------");
    }

    private static void testHBaseDualReadSample() {
        try {
            HBaseDualReadSample dualReadSample = new HBaseDualReadSample();
            dualReadSample.createConnection();
            dualReadSample.test();
        } catch (IOException e) {
            LOG.error("Failed to test HBase Dual Read because ", e);
        }
        LOG.info("-----------finish HBase Dual read -------------------");
    }

    private static void testGlobalSecondaryIndexSample() {
        try {
            GlobalSecondaryIndexSample gsiSample = new GlobalSecondaryIndexSample();
            gsiSample.test();
        } catch (IOException e) {
            LOG.error("Failed to test Global Secondary Index because ", e);
        }
        LOG.info("-----------finish Global Secondary index -------------------");
    }

    private static void testPhoenixSample() {
        try {
            PhoenixSample anotherSample = new PhoenixSample();
            anotherSample.test();
        } catch (SQLException e) {
            LOG.error("Failed to test Phoenix because ", e);
        }
        LOG.info("-----------finish Phoenix -------------------");
    }

    private static void login() throws IOException {
        Configuration clientConf = Utils.createClientConf();
        if (User.isHBaseSecurityEnabled(clientConf)) {
            // In Windows environment
            String userDir = TestMain.class.getClassLoader().getResource(Utils.CONF_DIRECTORY).getPath() + File.separator;
            // In Linux environment
            // String userDir = System.getProperty("user.dir") + File.separator + Utils.CONF_DIRECTORY + File.separator;

            String userKeytabFile = userDir + USER_KEYTAB_FILE;
            String krb5File = userDir + KRB5_CONF_FILE;
            /*
             * if need to connect zk, please provide jaas info about zk. of course,
             * you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath +
             * "jaas.conf"); but the demo can help you more : Note: if this process
             * will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, USER_NAME, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(USER_NAME, userKeytabFile, krb5File, clientConf);
        }
    }
}
