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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * π¶ƒ‹√Ë ˆ
 * hbase-example test multiple login main class
 *
 * @since 2013
 */

public class TestMultipleLogin {
    private static final Logger LOG = LoggerFactory.getLogger(TestMain.class.getName());
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

    private static String krb5File = null;
    private static String userName = null;
    private static String userKeytabFile = null;
    private static int CONF_DIR_NUM = 2;

    public static void main(String[] args) {
        List<String> confDirectorys = new ArrayList<>();
        List<Configuration> confs = new LinkedList<>();
        try {
            // conf directory
            confDirectorys.add("hadoopDomain");
            confDirectorys.add("hadoop1Domain");

            for (String confDir : confDirectorys) {
                confs.add(init(confDir));
            }

            // The conf directory which stored user.keytab and krb5conf
            login(confs.get(0), confDirectorys.get(0));
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }

        // test hbase
        try {
            int i = 1;
            for (Configuration conf : confs) {
                LOG.info("-----------Start HBase sample {} test-------------", i);
                HBaseSample oneSample = new HBaseSample(conf);
                oneSample.test();
                i++;
            }

        } catch (IOException e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish HBase -------------------");

        int j = 1;
        for (Configuration conf : confs) {
            // test phoenix
            LOG.info("-----------Start Phoenix sample {} test-------------", j);
            PhoenixSample anotherSample = new PhoenixSample(conf);
            anotherSample.test();
            j++;
        }

        LOG.info("-----------finish Phoenix -------------------");
    }

    private static void login(Configuration conf, String confDir) throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            userName = "hbaseuser1";
            //In Windows environment
            String userdir = TestMain.class.getClassLoader().getResource(confDir).getPath() + File.separator;
            //In Linux environment
            //String userdir = System.getProperty("user.dir") + File.separator + confDir + File.separator;

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

    private static Configuration init(String confDirectoryName) throws IOException {
        // Default load from conf directory
        Configuration conf = HBaseConfiguration.create();

        //In Windows environment
        String userdir = TestMain.class.getClassLoader().getResource(confDirectoryName).getPath() + File.separator;
        //In Linux environment
        //String userdir = System.getProperty("user.dir") + File.separator + confDirectoryName + File.separator;

        conf.addResource(new Path(userdir + "core-site.xml"), false);
        conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
        conf.addResource(new Path(userdir + "hbase-site.xml"), false);
        return conf;
    }
}
