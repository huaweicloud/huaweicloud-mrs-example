/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.redis.security;

import com.huawei.redis.Const;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * 安全认证使用方式三： 使用jaas文件配置方式
 *
 * 1. 必须设置redis.authentication.jaas, java.security.auth.login.config, java.security.krb5.conf 3个系统参数
 *   可通过java命令行-D参数设置
 *
 * jaas.conf文件样例:
 * Client{
 * com.sun.security.auth.module.Krb5LoginModule required
 * useKeyTab=true
 * keyTab="/xxx/user.keytab"
 * principal="xxxx"
 * useTicketCache=false
 * storeKey=true
 * debug=false;
 * };
 *
 * 2. 若Redis服务是全新安装服务，需要设置SERVER_REALM环境参数，该参数值为KrbServer服务配置项default_realm的值（该配置可到管理页面查询）。
 *    若Redis服务是由低版本升级而来，则不能设置该参数。
 *
 * @since 2020-09-30
 */
public class SecureJedisClusterDemo3 {
    public static void main(String[] args) {
        System.setProperty("redis.authentication.jaas", "true");
        System.setProperty("java.security.auth.login.config", "jaas.conf file path");
        System.setProperty("java.security.krb5.conf", "krb5.conf file path");
        // jaas.conf文件Section名字，不设置的话默认值为Client, 区分大小写
        // System.setProperty("redis.sasl.clientconfig", "redisClient");
        // System.setProperty("SERVER_REALM","HADOOP.COM");
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        hosts.add(new HostAndPort(Const.IP_1, Const.PORT_1));
        JedisCluster client = new JedisCluster(hosts, 5000);

        client.set("test-key", System.currentTimeMillis() + "");
        System.out.println(client.get("test-key"));
        client.del("test-key");
        client.close();
    }
}
