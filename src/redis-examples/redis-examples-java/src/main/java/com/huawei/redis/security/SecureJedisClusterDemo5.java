/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.redis.security;

import com.huawei.redis.CommonSslSocketFactory;
import com.huawei.redis.Const;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import javax.net.ssl.SSLSocketFactory;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

/**
 * 安全认证使用方式五： 使用用户名密码认证方式进行认证
 * <p>
 * 1、通过通过创建的用户名、密码方式进行安全认证
 * 2、创建JedisCluster对象，需要将独立的认证配置对象传入
 *
 * @since 2025-02-05
 */
public class SecureJedisClusterDemo5 {

    public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException {
        // 初始化套集群的认证信息
        Set<HostAndPort> hosts = new HashSet<>();
        hosts.add(new HostAndPort("ip1", 22400));
        Set<HostAndPort> hosts2 = new HashSet<>();
        hosts2.add(new HostAndPort("ip2", 22400));

        // 初始化连接池
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(3);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(100);

        // 创建ssl连接新消息
        boolean ssl = true;
        // 不校验校验服务端证书请使用以下接口
        final SSLSocketFactory socketFactory = CommonSslSocketFactory.createTrustALLSslSocketFactory();
        // 校验服务端证书请使用以下接口, 本地需要信任环境根证书
        // final SSLSocketFactory socketFactory = CommonSslSocketFactory.createSslSocketFactory();

        // 初始化jedisCluster连接
        int timeout = 5000;
        int maxAttempts = 2;
        JedisCluster client = new JedisCluster(hosts, timeout, timeout, maxAttempts, Const.userName, Const.password, null,
                jedisPoolConfig, ssl, socketFactory, null, null, null);

        client.set("test-key", "test-value");
        System.out.println(client.get("test-key"));
        client.del("test-key");
        client.close();
    }
}
