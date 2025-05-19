/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.javasampleapproach.redis.springdata;

import com.huawei.jredis.client.GlobalConfig;
import com.huawei.jredis.client.SslSocketFactoryUtil;
import com.huawei.jredis.client.auth.AuthConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.JedisPoolConfig;

import javax.net.ssl.SSLSocketFactory;
import java.time.Duration;

@Configuration
public class RedisClusterKerberosConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisClusterKerberosConfig.class.getName());

    @Value("${spring.redis.cluster.nodes}")
    private String nodes;

    @Value("${redis.keytab}")
    private String keytab;

    @Value("${redis.user}")
    private String user;

    @Value("${redis.krb5}")
    private String krb5;

    @Value("${redis.realm}")
    private String realm;

    @Value("${spring.redis.ssl}")
    private boolean ssl;

    @Value("${spring.redis.jedis.pool.max-active}")
    private int maxActive;

    @Value("${spring.redis.jedis.pool.max-idle}")
    private int maxIdle;

    @Value("${spring.redis.jedis.pool.min-idle}")
    private int minIdle;

    @Value("${spring.redis.jedis.pool.max-wait}")
    private int maxWait;

    @Bean
    public RedisClusterConfiguration kerberosClusterConfig() {
        RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration();
        String[] instances = nodes.split(",");
        for (String str : instances) {
            String[] nodeAndPort = str.split(":");
            clusterConfiguration.clusterNode(nodeAndPort[0], Integer.parseInt(nodeAndPort[1]));
        }
        return clusterConfiguration;
    }

    @Bean
    public JedisPoolConfig kerberosJedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMinIdle(minIdle);
        jedisPoolConfig.setMaxTotal(maxActive);
        jedisPoolConfig.setMaxWait(Duration.ofMillis(maxWait));
        return jedisPoolConfig;
    }

    @Bean
    public RedisConnectionFactory kerberosConnectionFactory() {
        SSLSocketFactory socketFactory = null;
        try {
            socketFactory = SslSocketFactoryUtil.createTrustALLSslSocketFactory();
        } catch (Exception e) {
            LOGGER.error("Failed to create the SSLSocketFactory object, the error is {}", e.getMessage());
            throw new RuntimeException(e);
        }
        JedisClientConfiguration jedisClientConfiguration = null;
        if (ssl) {
            jedisClientConfiguration = JedisClientConfiguration.builder()
                    .usePooling().poolConfig(kerberosJedisPoolConfig()).and().useSsl().sslSocketFactory(socketFactory).build();
        } else {
            jedisClientConfiguration = JedisClientConfiguration.builder()
                    .usePooling().poolConfig(kerberosJedisPoolConfig()).and().build();
        }
        return new JedisConnectionFactory(kerberosClusterConfig(), jedisClientConfiguration);
    }

    @Bean
    public RedisTemplate<String, Object> kerberosRedisTemplate() {
        System.setProperty("redis.authentication.jaas", "true");
        AuthConfiguration authConfig = new AuthConfiguration(krb5, keytab, user);
        GlobalConfig.setAuthConfiguration(authConfig);
        authConfig.setServerRealm(realm);
        authConfig.setLocalRealm(realm);
        LOGGER.info("user={}, realm={}", user, realm);

        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(kerberosConnectionFactory());
        return template;
    }
}
