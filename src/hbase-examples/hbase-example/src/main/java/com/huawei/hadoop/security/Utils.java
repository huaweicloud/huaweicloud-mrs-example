/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.File;

/**
 * Utils for sample building
 *
 * @since 2023-08-22
 */
public class Utils {
    private Utils() {
    }

    /**
     * Conf directory name
     */
    public static final String CONF_DIRECTORY = "conf";

    /**
     * Configuration file of core-site.xml
     */
    public static final String CLIENT_CORE_FILE = "core-site.xml";

    /**
     * Configuration file of hdfs-site.xml
     */
    public static final String CLIENT_HDFS_FILE = "hdfs-site.xml";

    /**
     * Configuration file of hbase-site.xml
     */
    public static final String CLIENT_HBASE_FILE = "hbase-site.xml";

    /**
     * Properties for enabling encrypted HBase ZooKeeper communication
     */
    private static final String ZK_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";

    private static final String ZK_CLIENT_SECURE = "zookeeper.client.secure";

    private static final String ZK_SSL_SOCKET_CLASS = "org.apache.zookeeper.ClientCnxnSocketNetty";

    /**
     * If SSL-encrypted communication is enabled for ZooKeeper in the cluster, HBase needs to perform adaptation.
     *
     * @param conf See {@link Configuration}
     */
    public static void handleZkSslEnabled(Configuration conf) {
        if (conf == null) {
            return;
        }
        boolean zkSslEnabled = conf.getBoolean("HBASE_ZK_SSL_ENABLED", false);
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

    /**
     * Create a client configuration according to client file.
     *
     * @return Client Configuration.
     */
    public static Configuration createClientConf() {
        // In Windows environment
        String userDir = Utils.class.getClassLoader().getResource(CONF_DIRECTORY).getPath() + File.separator;
        // In Linux environment
        // String userDir = System.getProperty("user.dir") + File.separator + CONF_DIRECTORY + File.separator;
        return createConfByUserDir(userDir);
    }

    /**
     * Create configuration according to the client conf file in userDir
     *
     * @param userDir The directory contains client conf file
     * @return Configuration
     */
    public static Configuration createConfByUserDir(String userDir) {
        // Default load from conf directory
        Configuration conf = HBaseConfiguration.create();
        if (userDir == null || userDir.isEmpty()) {
            return conf;
        }
        conf.addResource(new Path(userDir + CLIENT_CORE_FILE), false);
        conf.addResource(new Path(userDir + CLIENT_HDFS_FILE), false);
        conf.addResource(new Path(userDir + CLIENT_HBASE_FILE), false);
        return conf;
    }
}
