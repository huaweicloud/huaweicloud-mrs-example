package com.huawei.bigdata.mapreduce.tools;

import org.apache.hadoop.conf.Configuration;

public class Utils {
    /**
     * Properties for enabling encrypted HBase ZooKeeper communication
     */
    private static final String ZK_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";

    private static final String ZK_CLIENT_SECURE = "zookeeper.client.secure";

    private static final String ZK_SSL_SOCKET_CLASS = "org.apache.zookeeper.ClientCnxnSocketNetty";

    public static void handleZkSslEnabled(Configuration conf) {
        if (conf == null) {
            return;
        }
        boolean zkSslEnabled = conf.getBoolean("zk_ssl_enabled", false);
        if (zkSslEnabled) {
            System.setProperty(ZK_CLIENT_CNXN_SOCKET, ZK_SSL_SOCKET_CLASS);
            System.setProperty(ZK_CLIENT_SECURE, "true");
        } else {
            if (System.getProperty(ZK_CLIENT_CNXN_SOCKET) != null) {
                System.clearProperty(ZK_CLIENT_CNXN_SOCKET);
            }
            if (System.getProperty(ZK_CLIENT_SECURE) != null) {
                System.clearProperty(ZK_CLIENT_SECURE);
            }
        }
    }
}
