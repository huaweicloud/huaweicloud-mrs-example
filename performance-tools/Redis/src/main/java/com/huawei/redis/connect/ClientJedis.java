/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.connect;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClientJedis implements ClientCommands {
    private JedisPool jedisPool;

    private Jedis clientPipe = null;

    private Pipeline pipeline = null;

    public ClientJedis(String host, int port) {
        this.jedisPool = new JedisPool(host, port);
        this.clientPipe = new Jedis(host, port);
        this.pipeline = this.clientPipe.pipelined();
    }

    public String get(String key, boolean isPipe) {
        if (!isPipe) {
            try (Jedis client = this.jedisPool.getResource()) {
                return client.get(key);
            }
        }
        return this.pipeline.get(key).get();
    }

    public String set(String key, String value, boolean isPipe) {
        if (!isPipe) {
            try (Jedis client = this.jedisPool.getResource()) {
                return client.set(key, value);
            }
        }
        return this.pipeline.set(key, value).get();
    }

    public String hget(String key, String field, boolean isPipe) {
        if (!isPipe) {
            try (Jedis client = this.jedisPool.getResource()) {
                return client.hget(key, field);
            }
        }
        return this.pipeline.hget(key, field).get();
    }

    public String hmset(String key, Map<String, String> value, boolean isPipe) {
        if (!isPipe) {
            try (Jedis client = this.jedisPool.getResource()) {
                return client.hmset(key, value);
            }
        }
        return this.pipeline.hmset(key, value).get();
    }

    public Long rpush(String key, String value, boolean isPipe) {
        if (!isPipe) {
            Jedis client = this.jedisPool.getResource();
            return client.rpush(key, value);
        }
        return this.pipeline.rpush(key, new String[] {value}).get();
    }

    public List<String> lrange(String key, Long start, Long end, boolean isPipe) {
        if (!isPipe) {
            Jedis client = this.jedisPool.getResource();
            return client.lrange(key, start, end);
        }
        return this.pipeline.lrange(key, start, end).get();
    }

    public Long sadd(String key, String value, boolean isPipe) {
        if (!isPipe) {
            Jedis client = this.jedisPool.getResource();
            return client.sadd(key, value);
        }
        return this.pipeline.sadd(key, new String[] {value}).get();
    }

    public Set<String> zrange(String key, Long start, Long end, boolean isPipe) {
        if (!isPipe) {
            Jedis client = this.jedisPool.getResource();
            return client.zrange(key, start, end);
        }
        return this.pipeline.zrange(key, start, end).get();
    }

    public Set<String> smembers(String key, boolean isPipe) {
        if (!isPipe) {
            Jedis client = this.jedisPool.getResource();
            return client.smembers(key);
        }
        return this.pipeline.smembers(key).get();
    }

    public Long zadd(String key, Map<String, Double> value, boolean isPipe) {
        if (!isPipe) {
            Jedis client = this.jedisPool.getResource();
            return client.zadd(key, value);
        }
        return this.pipeline.zadd(key, value).get();
    }

    public Boolean exists(String key, boolean isPipe) {
        if (!isPipe) {
            try (Jedis client = this.jedisPool.getResource()) {
                return client.exists(key);
            }
        }
        return this.pipeline.exists(key).get();
    }

    public List<Object> syncAndReturnAll() {
        return this.pipeline.syncAndReturnAll();
    }

    public void close() {
        this.clientPipe.close();
        this.jedisPool.close();
    }
}
