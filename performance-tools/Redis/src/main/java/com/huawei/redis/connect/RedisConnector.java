/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.connect;

import com.huawei.jredis.client.GlobalConfig;
import com.huawei.jredis.client.auth.AuthConfiguration;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class RedisConnector implements ClientCommands {

    private static final Logger LOG = Logger.getLogger(RedisConnector.class);

    private static final String CFG_FILE = "jedis.properties";

    private static final String CONNECT_MODE = "redis.connect.model";

    private static final String REDIS_HOST_LIST = "redis.host.list";

    private static final String REDIS_CLIENT_TIMEOUT = "redis.client.timeout";

    private static final String REDIS_CLIENT_POOL = "redis.client.pool.max";

    private static final String TEST_SSL = "redis.client.ssl";

    private static final String CLIENT_POOL_MIN = "redis.client.pool.min";

    private static final String LOGIN_USER = "loginUser";

    private static final String KRB5_PATH = "krb5.path";

    private static final String KEYTAB_PATH = "keytab.path";

    private static final String SERVER_REAL = "serverReal";

    private static final String CACHE_LOGIN = "cacheLogin";

    private ClientCommands client = null;

    private static Set<HostAndPort> redisHosts = new HashSet<>();

    private static int connMode = 1;

    private static int timeout = 1500;

    private static int maxPool = 8;

    private static int minPool = 5;

    private static boolean ssl = false;

    private static void initClientCfg() {

        Properties properties = new Properties();

        InputStream in = null;
        try {
            in = new FileInputStream(new File(CFG_FILE));
            properties.load(in);
            connMode = Integer.parseInt(properties.getProperty(CONNECT_MODE), 10);
            timeout = Integer.parseInt(properties.getProperty(REDIS_CLIENT_TIMEOUT), 10);
            maxPool = Integer.parseInt(properties.getProperty(REDIS_CLIENT_POOL), 10);
            ssl = Boolean.parseBoolean(properties.getProperty(TEST_SSL));
            minPool = Integer.parseInt(properties.getProperty(CLIENT_POOL_MIN), 5);
            String user = properties.getProperty(LOGIN_USER);
            String krb5Path = properties.getProperty(KRB5_PATH);
            String keytabPath = properties.getProperty(KEYTAB_PATH);
            boolean cacheLogin = Boolean.parseBoolean(properties.getProperty(CACHE_LOGIN));
            String serverReal = properties.getProperty(SERVER_REAL, "HADOOP.COM");
            System.setProperty("SERVER_REALM", serverReal);

            AuthConfiguration configuration = new AuthConfiguration(krb5Path, keytabPath, user);
            configuration.setCacheLogin(cacheLogin);
            GlobalConfig.setAuthConfiguration(configuration);
            String redisInstancesList = properties.getProperty(REDIS_HOST_LIST).trim();
            String[] redisInstances = redisInstancesList.split(",");
            for (String redisInstance : redisInstances) {
                String[] hostPortPair = redisInstance.trim().split(":");
                String host = hostPortPair[0].trim();
                int port = Integer.parseInt(hostPortPair[1].trim());
                if (!host.equals("") && port != 0) {
                    redisHosts.add(new HostAndPort(host, port));
                }
            }
        } catch (IOException e) {

            LOG.error("Failed to load redis client cfg file", e);
            try {

                in.close();
            } catch (IOException localIOException1) {
            }
        } finally {
            try {
                in.close();
            } catch (IOException localIOException2) {
            }
        }
    }

    public void close() {
        try {

            this.client.close();
        } catch (Exception e) {
            LOG.error("Failed to close Redis Connector", e);
        }
    }

    public RedisConnector() {
        initClientCfg();

        if (this.client == null) {
            if (connMode == 0) {
                HostAndPort hostAndPort = redisHosts.iterator().next();
                try {
                    this.client = new ClientJedis(hostAndPort.getHost(), hostAndPort.getPort());
                } catch (Exception e) {
                    LOG.error("Failed to init SingleAdapter", e);
                }
            } else {
                try {
                    for (HostAndPort redisHost : redisHosts) {
                        LOG.info("redisHosts: " + redisHost);
                    }

                    JedisPoolConfig jpool = new JedisPoolConfig();
                    jpool.setMaxTotal(maxPool);
                    jpool.setMaxIdle(maxPool);
                    jpool.setMinIdle(minPool);
                    jpool.setTestOnBorrow(true);
                    this.client = new ClientJedisCluster(redisHosts, timeout, jpool, ssl);
                } catch (Exception e) {
                    LOG.error("Failed to init ClusterAdapter", e);
                }
            }
        }
    }

    public static RedisConnector getInstance() {
        try {
            return new RedisConnector();
        } catch (Exception e) {
            LOG.error("Failed to create Redis Connector", e);
        }
        return null;
    }

    public String get(String key, boolean isPipe) throws Exception {
        return this.client.get(key, isPipe);
    }

    public String set(String key, String value, boolean isPipe) throws Exception {
        return this.client.set(key, value, isPipe);
    }

    public String hget(String key, String field, boolean isPipe) throws Exception {
        return this.client.hget(key, field, isPipe);
    }

    public String hmset(String key, Map<String, String> value, boolean isPipe) throws Exception {

        return this.client.hmset(key, value, isPipe);
    }

    public Long rpush(String key, String value, boolean isPipe) throws Exception {

        return this.client.rpush(key, value, isPipe);
    }

    public List<String> lrange(String key, Long start, Long end, boolean isPipe) throws Exception {

        return this.client.lrange(key, start, end, isPipe);
    }

    public Long sadd(String key, String value, boolean isPipe) throws Exception {

        return this.client.sadd(key, value, isPipe);
    }

    public Set<String> smembers(String key, boolean isPipe) throws Exception {

        return this.client.smembers(key, isPipe);
    }

    public Long zadd(String key, Map<String, Double> value, boolean isPipe) throws Exception {

        return this.client.zadd(key, value, isPipe);
    }

    public Set<String> zrange(String key, Long start, Long end, boolean isPipe) throws Exception {

        return this.client.zrange(key, start, end, isPipe);
    }

    public Boolean exists(String key, boolean isPipe) throws Exception {

        return this.client.exists(key, isPipe);
    }

    public List<Object> syncAndReturnAll() throws Exception {

        return this.client.syncAndReturnAll();
    }
}
