/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.oozie;

import com.huawei.bigdata.hadoop.security.KerberosUtil;
import com.huawei.bigdata.hadoop.security.LoginUtil;
import com.huawei.bigdata.utils.Helper;
import com.huawei.bigdata.utils.PropertiesCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

/**
 * Oozie Example for Security Mode using Java API
 *
 * @since 2021-01-25
 */
public class OozieRestApiMain {
    private static final Logger logger = LoggerFactory.getLogger(OozieRestApiMain.class);

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static final String ZOOKEEPER_DEFAULT_SERVER_NAME = "zookeeper";

    private static String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL;

    private static final String KERBEROS_PRINCIPAL = "username.client.kerberos.principal";

    private static final String KEYTAB_FILE = "username.client.keytab.file";

    public static void main(String[] args) {
        try {
            String resPath = Helper.getResourcesPath();

            String krb5FilePath = resPath + "krb5.conf";
            String keytabFilePath = resPath + "developuser.keytab";
            String user = PropertiesCache.getInstance().getProperty("submit_user");

            System.setProperty("java.security.krb5.conf", krb5FilePath);
            ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL =
                    ZOOKEEPER_DEFAULT_SERVER_NAME + "/hadoop." + KerberosUtil.getKrb5DomainRealm();

            login(keytabFilePath, krb5FilePath, user);

            logger.info("current user is : {} ", UserGroupInformation.getCurrentUser());
            logger.info("login user is : {} ", UserGroupInformation.getLoginUser());

            String jobFilePath = resPath + "job.properties";

            new OozieSample().test(jobFilePath);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        logger.info("----------finish Oozie -----------------");
    }

    private static void login(String keytabFilePath, String krb5FilePath, String user) throws IOException {
        Configuration conf = new Configuration();
        conf.set(KERBEROS_PRINCIPAL, user);
        conf.set(KEYTAB_FILE, keytabFilePath);
        conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");

        /*
         * if need to connect zk, please provide jaas info about zk. of course,
         * you can do it as below:
         * System.setProperty("java.security.auth.login.config", confDirPath +
         * "jaas.conf"); but the demo can help you more : Note: if this process
         * will connect more than one zk cluster, the demo may be not proper. you
         * can contact us for more help
         */
        LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, user, keytabFilePath);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.login(user, keytabFilePath, krb5FilePath, conf);
    }
}
