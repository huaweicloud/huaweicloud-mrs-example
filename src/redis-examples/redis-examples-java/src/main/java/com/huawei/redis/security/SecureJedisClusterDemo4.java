/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.redis.security;

import com.huawei.jredis.client.auth.AuthConfiguration;
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
 * 安全认证使用方式四： API设置独立认证信息，可以在一个应用里面使用多个认证用户访问多个Redis集群
 * <p>
 * 1、通过krb5.conf、keytab路径、principal、服务端域名等初始化认证配置对象
 * 2、创建JedisCluster对象，需要将独立的认证配置对象传入
 *
 * @since 2022-02-16
 */
public class SecureJedisClusterDemo4 {

    /**
     * 默认连接超时时间
     */
    private static final Integer TIMEOUT = 3000;

    /**
     * 最大重试次数
     */
    private static final Integer MAX_ATTEMPTS = 1;

    public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException {
        // 初始化kerberos认证对象
        AuthConfiguration authConfiguration = new AuthConfiguration("path1/krb5.conf", "path1/user.keytab",
                "user@HADOOP1.COM");
        authConfiguration.setServerRealm("HADOOP1.COM");
        authConfiguration.setLocalRealm("HADOOP1.COM");
        AuthConfiguration authConfiguration2 = new AuthConfiguration("path2/krb5.conf", "path2/user.keytab",
                "user@HADOOP.COM");
        authConfiguration.setServerRealm("HADOOP.COM");
        authConfiguration.setLocalRealm("HADOOP.COM");
        // 初始化第二套集群的认证信息
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
        boolean ssl = false;
        // 不校验校验服务端证书请使用以下接口
        final SSLSocketFactory socketFactory = CommonSslSocketFactory.createTrustALLSslSocketFactory();
        // 校验服务端证书请使用以下接口, 本地需要信任环境根证书
        // final SSLSocketFactory socketFactory = CommonSslSocketFactory.createSslSocketFactory();

        // 初始化jedisCluster连接
        writeCluster(hosts, jedisPoolConfig, socketFactory, ssl, authConfiguration);
        // 连接第二个集群的JedisCluster连接
        writeCluster(hosts2, jedisPoolConfig, socketFactory, ssl, authConfiguration2);

    }

    /**
     * 连接Redis集群
     *
     * @param hosts             主实例列表
     * @param jedisPoolConfig   连接池配置
     * @param socketFactory     ssl 忽略证书的SSLSocketFactory
     * @param ssl               是否开启ssl
     * @param authConfiguration 安全认证配置信息
     */
    private static void writeCluster(Set<HostAndPort> hosts, JedisPoolConfig jedisPoolConfig,
            SSLSocketFactory socketFactory, boolean ssl, AuthConfiguration authConfiguration) {
        JedisCluster client = new JedisCluster(hosts, TIMEOUT, TIMEOUT, TIMEOUT, MAX_ATTEMPTS, jedisPoolConfig, ssl,
                socketFactory, authConfiguration);
        client.set("test-key", System.currentTimeMillis() + "");
        System.out.println(client.get("test-key"));
        client.del("test-key");
        client.close();
    }
}
