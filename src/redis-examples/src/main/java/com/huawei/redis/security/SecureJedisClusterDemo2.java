/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.redis.security;

import com.huawei.jredis.client.GlobalConfig;
import com.huawei.jredis.client.auth.AuthConfiguration;
import com.huawei.redis.Const;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * 安全认证使用方式二： API设置方式
 *
 * 1. 构造AuthConfiguration对象，指定keytab文件路径，principal，krb5.conf文件路径
 *    若krb5.conf文件路径不指定，默认读取java.security.krb5.conf系统参数的值
 *
 * 2. GlobalConfig.setAuthConfiguration设置为上面创建的AuthConfiguration对象
 *
 * 3. 创建JedisCluster对象，创建方式同非安全一样
 *
 * 4. 若Redis服务是全新安装服务，需要设置SERVER_REALM环境参数，该参数值为KrbServer服务配置项default_realm的值（该配置可到管理页面查询）。
 *    若Redis服务是由低版本升级而来，则不能设置该参数。
 *
 * @since 2020-09-30
 */
public class SecureJedisClusterDemo2 {
    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf", "krb5.conf file path");
        AuthConfiguration authConfiguration = new AuthConfiguration("keytab file path", "principal");
        GlobalConfig.setAuthConfiguration(authConfiguration);
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
