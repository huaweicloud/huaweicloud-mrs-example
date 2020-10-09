/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.oozie.examples;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import com.huawei.bigdata.security.KerberosUtil;
import com.huawei.bigdata.security.LoginUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Oozie Example for Security Mode using Java API
 *
 * @since 2020-09-30
 */
public class OozieMain {
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String ZOOKEEPER_DEFAULT_SERVER_NAME = "zookeeper";

    private static String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL;

    private static String KERBEROS_PRINCIPAL = "username.client.kerberos.principal";

    private static String KEYTAB_FILE = "username.client.keytab.file";

    private static boolean SECURITY_CLUSTER = true;

    private static Configuration conf = null;

    public static void main(String[] args) {
        try {
            String krb5File = Constant.APPLICATION_PATH + "krb5.conf";
            System.setProperty("java.security.krb5.conf", krb5File);
            ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL =
                    ZOOKEEPER_DEFAULT_SERVER_NAME + "/hadoop." + KerberosUtil.getKrb5DomainRealm();

            login();

            System.out.println("current user is " + UserGroupInformation.getCurrentUser());
            System.out.println("login user is " + UserGroupInformation.getLoginUser());

            new OozieSample(SECURITY_CLUSTER).test();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        System.out.println("-----------finish Oozie -------------------");
    }

    private static void login() throws IOException {
        String userKeytabFile = Constant.APPLICATION_PATH + "user.keytab";
        String krb5File = Constant.APPLICATION_PATH + "krb5.conf";

        conf = new Configuration();
        conf.set(KERBEROS_PRINCIPAL, Constant.SUBMIT_USER);
        conf.set(KEYTAB_FILE, userKeytabFile);
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
        LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, Constant.SUBMIT_USER, userKeytabFile);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.login(Constant.SUBMIT_USER, userKeytabFile, krb5File, conf);
    }
}
