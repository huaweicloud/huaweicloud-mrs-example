/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.utils;

import com.huawei.bigdata.spark.SparkUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * HBase login user util
 *
 * @since 2021-01-25
 */
public class HBaseUtil {

    private static final String ZK_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";
    private static final String ZK_CLIENT_SECURE = "zookeeper.client.secure";
    private static final String ZK_SSL_SOCKET_CLASS = "org.apache.zookeeper.ClientCnxnSocketNetty";

    /**
     * create login user for HBase connection
     *
     * @return login user
     */
    public static User getAuthenticatedUser() {
        User loginUser = null;
        SparkUtil.login();

        try {
            loginUser =  User.create(UserGroupInformation.getLoginUser());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return loginUser;
    }

    public static void zkSslUtil(Configuration config) {
        boolean sslEnabled = config.getBoolean("HBASE_ZK_SSL_ENABLED", false);
        if (sslEnabled) {
            System.setProperty(ZK_CLIENT_CNXN_SOCKET, ZK_SSL_SOCKET_CLASS);
            System.setProperty(ZK_CLIENT_SECURE, "true");
        } else {
            if(System.getProperty(ZK_CLIENT_CNXN_SOCKET) != null) {
                System.clearProperty(ZK_CLIENT_CNXN_SOCKET);
            }
            if(System.getProperty(ZK_CLIENT_SECURE) != null) {
                System.clearProperty(ZK_CLIENT_SECURE);
            }
        }
    }
}
