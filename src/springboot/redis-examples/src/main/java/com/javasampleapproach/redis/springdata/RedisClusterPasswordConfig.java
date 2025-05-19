/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.javasampleapproach.redis.springdata;

import com.huawei.jredis.client.SslSocketFactoryUtil;
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
public class RedisClusterPasswordConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisClusterPasswordConfig.class.getName());

    @Value("${spring.redis.cluster.nodes}")
    private String nodes;

    @Value("${spring.redis.password}")
    private String password;

    @Value("${spring.redis.username}")
    private String username;

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
    public RedisClusterConfiguration redisClusterConfiguration() {
        RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration();
        String[] instances = nodes.split(",");
        for (String str : instances) {
            String[] nodeAndPort = str.split(":");
            clusterConfiguration.clusterNode(nodeAndPort[0], Integer.parseInt(nodeAndPort[1]));
        }
        clusterConfiguration.setUsername(username);
        clusterConfiguration.setPassword(password);
        return clusterConfiguration;
    }

    @Bean
    public JedisPoolConfig jedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMinIdle(minIdle);
        jedisPoolConfig.setMaxTotal(maxActive);
        jedisPoolConfig.setMaxWait(Duration.ofMillis(maxWait));
        return jedisPoolConfig;
    }

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        JedisClientConfiguration jedisClientConfiguration = null;
        SSLSocketFactory socketFactory = null;
        try {
            socketFactory = SslSocketFactoryUtil.createTrustALLSslSocketFactory();
        } catch (Exception e) {
            LOGGER.error("Failed to create the SSLSocketFactory object, the error is {}", e.getMessage());
            throw new RuntimeException(e);
        }
        if (ssl) {
            jedisClientConfiguration = JedisClientConfiguration.builder()
                    .usePooling().poolConfig(jedisPoolConfig()).and().useSsl().sslSocketFactory(socketFactory).build();
        } else {
            jedisClientConfiguration = JedisClientConfiguration.builder()
                    .usePooling().poolConfig(jedisPoolConfig()).build();
        }
        return new JedisConnectionFactory(redisClusterConfiguration(), jedisClientConfiguration);
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory());
        return template;
    }
}
