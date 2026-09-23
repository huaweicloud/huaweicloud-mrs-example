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
            String host;
            int port;
            try {
                if (str.startsWith("[")) {
                    int closeBracket = str.indexOf("]");
                    if (closeBracket < 0) {
                        LOGGER.warn("Invalid IPv6 node format, missing closing bracket: {}", str);
                        continue;
                    }
                    host = str.substring(0, closeBracket + 1);
                    String portPart = str.substring(closeBracket + 1);
                    if (portPart.startsWith(":")) {
                        port = Integer.parseInt(portPart.substring(1));
                    } else if (portPart.isEmpty()) {
                        LOGGER.warn("Invalid node format, missing port: {}", str);
                        continue;
                    } else {
                        LOGGER.warn("Invalid node format after bracket: {}", str);
                        continue;
                    }
                } else {
                    int lastColonIndex = str.lastIndexOf(":");
                    if (lastColonIndex > 0) {
                        host = str.substring(0, lastColonIndex);
                        port = Integer.parseInt(str.substring(lastColonIndex + 1));
                    } else {
                        LOGGER.warn("Invalid node format, missing port: {}", str);
                        continue;
                    }
                }
                clusterConfiguration.clusterNode(host, port);
            } catch (NumberFormatException e) {
                LOGGER.warn("Failed to parse port for node: {}, error: {}", str, e.getMessage());
            }
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
