/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.connect;

import com.huawei.medis.ClusterBatch;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLSocketFactory;

public class ClientJedisCluster implements ClientCommands {
    JedisCluster client = null;

    ClusterBatch clientPipe = null;

    private int maxAttempts = 2;

    public ClientJedisCluster(Set<HostAndPort> hosts, int timeout, JedisPoolConfig poolConfig, boolean ssl)
        throws Exception {

        final SSLSocketFactory socketFactory = SSLSocketFactoryUtil.createTrustALLSslSocketFactory();
        this.client = new JedisCluster(hosts, timeout, timeout, maxAttempts, "", "", poolConfig, ssl, socketFactory,
            null, null, null);

        this.clientPipe = this.client.getPipeline();

    }

    public ClientJedisCluster(Set<HostAndPort> hosts, int timeout, boolean ssl) throws Exception {

        final SSLSocketFactory socketFactory = SSLSocketFactoryUtil.createTrustALLSslSocketFactory();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        this.client = new JedisCluster(hosts, timeout, timeout, maxAttempts, "", "", poolConfig, ssl, socketFactory,
            null, null, null);

        this.clientPipe = this.client.getPipeline();

    }

    public String get(String key, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.get(key);

        }

        return this.clientPipe.get(key).get();

    }

    public String set(String key, String value, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.set(key, value);

        }

        return this.clientPipe.set(key, value).get();

    }

    public String hget(String key, String field, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.hget(key, field);

        }

        return this.clientPipe.hget(key, field).get();

    }

    public String hmset(String key, Map<String, String> value, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.hmset(key, value);

        }

        return this.clientPipe.hmset(key, value).get();

    }

    public Long rpush(String key, String value, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.rpush(key, value);

        }

        return this.clientPipe.rpush(key, new String[] {value}).get();

    }

    public List<String> lrange(String key, Long start, Long end, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.lrange(key, start, end);

        }

        return this.clientPipe.lrange(key, start, end).get();

    }

    public Long sadd(String key, String value, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.sadd(key, value);

        }

        return this.clientPipe.sadd(key, new String[] {value}).get();

    }

    public Set<String> smembers(String key, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.smembers(key);

        }

        return this.clientPipe.smembers(key).get();

    }

    public Long zadd(String key, Map<String, Double> value, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.zadd(key, value);

        }

        return this.clientPipe.zadd(key, value).get();

    }

    public Set<String> zrange(String key, Long start, Long end, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.zrange(key, start, end);

        }

        return this.clientPipe.zrange(key, start, end).get();

    }

    public Boolean exists(String key, boolean isPipe) throws Exception {

        if (!isPipe) {

            return this.client.exists(key);

        }

        return this.clientPipe.exists(key).get();

    }

    public List<Object> syncAndReturnAll() throws Exception {

        return this.clientPipe.syncAndReturnAll();

    }

    public void close() {

        this.client.close();

    }

}
