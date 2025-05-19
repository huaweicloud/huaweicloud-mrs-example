/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.mrs;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.IOException;

/**
 * @since 2024-11-04
 */
public class ZkReadWrite {

    // zk的连接地址
    private static final String zkQurom = "";

    public static void setup(String user, String confDir, String realm) throws IOException {
        File keytab = new File(confDir, "user.keytab");
        File krb5conf = new File(confDir, "krb5.conf");
        LoginUtil.setJaasFile(user, keytab.getCanonicalPath());
        LoginUtil.setKrb5Config(krb5conf.getCanonicalPath());
        System.setProperty("zookeeper.sessionRequireClientSASLAuth", "true");
        System.setProperty("zookeeper.allowSaslFailedClients", "true");
        System.setProperty("zookeeper.server.principal", "zookeeper/hadoop." + realm);
    }

    private static void processSync(WatchedEvent watchedEvent, String name) {
        switch (watchedEvent.getType()) {
            case None:
                System.out.println("[Watcher "+ name + "] Connected");
                break;
            case NodeDataChanged:
                System.out.println("[Watcher "+ name + "] Node data changed, path is " + watchedEvent.getPath());
                break;
            case NodeCreated:
                System.out.println("[Watcher "+ name + "] Node created, path is " + watchedEvent.getPath());
                break;
            case NodeDeleted:
                System.out.println("[Watcher "+ name + "] Node deleted, path is " + watchedEvent.getPath());
                break;
            default:
                System.out.println(watchedEvent);
        }
    }

    private static Watcher getWatcher(String name) {
        return watchedEvent -> {
            switch (watchedEvent.getState()) {
                case SyncConnected:
                    processSync(watchedEvent, name);
                    break;
                case Closed:
                    System.out.println("[Watcher "+ name + "] Connect closed");
                    break;
                case SaslAuthenticated:
                    System.out.println("[Watcher "+ name + "] kerberos auth succeed");
                    break;
                default:
                    System.out.println("[Watcher "+ name + "] State=" + watchedEvent.getState());

            }
        };
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZkReadWrite.setup("admintest", "", "hadoop.huawei.com");
        String zkPath = "/zk_demo";
        try (ZooKeeper client = new ZooKeeper(zkQurom, 30000, getWatcher("Connection"))) {
            Watcher zNode = getWatcher("ZNode");
            client.getChildren("/", false);
            // client.addWatch(zkPath, zNode, AddWatchMode.PERSISTENT_RECURSIVE);
            System.out.println("=============== begin create dir ==============");
            // 创建zkznode，并且制定权限
            client.create(zkPath, "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            // 读取zknode的值
            System.out.println("before updated, value is "  );
            System.out.println("================" + new String(client.getData(zkPath, zNode, null)));
            // 设置值
            client.setData(zkPath, "2".getBytes(), -1);
            // 读取zknode的值
            System.out.println("before updated, value is  " + new String(client.getData(zkPath, zNode, null)));
            // 删除znode
            client.delete(zkPath, -1);
        }
    }

}
