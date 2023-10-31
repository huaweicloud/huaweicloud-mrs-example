/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.redis.demo;

import com.huawei.jredis.client.KerberosUtil;
import com.huawei.medis.ClusterBatch;
import com.huawei.redis.CommonSslSocketFactory;
import com.huawei.redis.Const;
import com.huawei.redis.LoginUtil;

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLSocketFactory;

/**
 * 测试类
 *
 * @since 2020-09-30
 */
public class RedisTest {
    /**
     * logger
     */
    private static final Logger LOGGER = Logger.getLogger(RedisTest.class);

    /**
     * client
     */
    private JedisCluster client;

    /**
     * pipeline
     */
    private ClusterBatch pipeline;

    /**
     * principal
     */
    private static String principal;

    /**
     * 构造函数
     */
    public RedisTest() throws Exception {
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        hosts.add(new HostAndPort(Const.IP_1, Const.PORT_1));
        hosts.add(new HostAndPort(Const.IP_2, Const.PORT_2));
        // add more host...
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // 不校验校验服务端证书请使用以下接口
        final SSLSocketFactory socketFactory = CommonSslSocketFactory.createTrustALLSslSocketFactory();
        // 校验服务端证书请使用以下接口
        // final SSLSocketFactory socketFactory = CommonSslSocketFactory.createSslSocketFactory();

        // socket timeout, unit: ms
        int timeout = 5000;
        int maxAttempts = 2;
        boolean ssl = false;
        client = new JedisCluster(hosts, timeout, timeout, maxAttempts,"","", poolConfig, ssl, socketFactory, null, null, null);
    }

    /**
     * destroy 方法，销毁资源
     */
    public void destory() {
        if (pipeline != null) {
            pipeline.close();
        }

        if (client != null) {
            client.close();
        }
    }

    /**
     * geo 测试
     */
    public void testGeo() {
        Map<String, GeoCoordinate> locationMap = new HashMap<String, GeoCoordinate>();
        locationMap.put("test1", new GeoCoordinate(10, 30));
        locationMap.put("test2", new GeoCoordinate(11, 30));
        locationMap.put("test3", new GeoCoordinate(11, 31));
        locationMap.put("test4", new GeoCoordinate(12, 31));
        locationMap.put("test5", new GeoCoordinate(13, 35));

        client.geoadd("location", locationMap);

        double dis = client.geodist("location", "test1", "test2", GeoUnit.KM);
        LOGGER.info("The distance between test1 and test2 is {} " + dis + " KM.");

        List<GeoCoordinate> geoCoordinateList = client.geopos("location", "test1", "test2");
        LOGGER.info("Geo location info is " + geoCoordinateList.toString());

        List<String> hashInfo = client.geohash("location", "test1", "test2");
        LOGGER.info("geohash info of test1 and test2 is " + hashInfo.toString());

        List<GeoRadiusResponse> memberInfoByNumber =
            client.georadius("location", 11, 31, 500, GeoUnit.KM, GeoRadiusParam.geoRadiusParam().withDist());

        LOGGER.info("Get location info by longitude and latitude : ");
        for (GeoRadiusResponse geoRadiusResponse : memberInfoByNumber) {
            LOGGER.info(geoRadiusResponse.getMemberByString() + " : " + geoRadiusResponse.getDistance());
        }

        List<GeoRadiusResponse> memberInfoByLocation =
            client.georadiusByMember(
                "location",
                "test3",
                500,
                GeoUnit.KM,
                GeoRadiusParam.geoRadiusParam().sortAscending().withDist().count(3));

        LOGGER.info("Get location info by member : ");
        for (GeoRadiusResponse geoRadiusResponse : memberInfoByLocation) {
            LOGGER.info(geoRadiusResponse.getMemberByString() + " : " + geoRadiusResponse.getDistance());
        }
    }

    /**
     * 字符串测试
     */
    public void testString() {
        String key = "sid-user01";

        // Save user's session ID, and set expire time
        client.setex(key, 5, "A0BC9869FBC92933255A37A1D21167B2");
        String sessionId = client.get(key);
        LOGGER.info("User " + key + ", session id: " + sessionId);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            LOGGER.warn("InterruptedException");
        }

        sessionId = client.get(key);
        LOGGER.info("User " + key + ", session id: " + sessionId);

        key = "message";

        client.set(key, "hello");
        String value = client.get(key);
        LOGGER.info("Value: " + value);

        client.append(key, " world");
        value = client.get(key);
        LOGGER.info("After append, value: " + value);

        client.del(key);

        SetParams params = new SetParams().ex(1000);
        client.set(key, "A0BC9869FBC92933255A37A1D21167B2", params);
        client.touch(key);
        LOGGER.info("User " + key + ", session id: " + client.get(key));

        client.del(key);
    }

    /**
     * list 类型测试
     */
    public void testList() {
        String key = "messages";

        // Right push
        client.rpush(key, "Hello how are you?");
        client.rpush(key, "Fine thanks. I'm having fun with redis.");
        client.rpush(key, "I should look into this NOSQL thing ASAP");

        // Fetch all data
        List<String> messages = client.lrange(key, 0, -1);
        LOGGER.info("All messages: " + messages);

        long len = client.llen(key);
        LOGGER.info("Message count: " + len);

        // Fetch the first element and delete it from list
        String message = client.lpop(key);
        LOGGER.info("First message: " + message);
        len = client.llen(key);
        LOGGER.info("After one pop, message count: " + len);

        client.del(key);
    }

    /**
     * hash 类型测试
     */
    public void testHash() {
        String key = "userinfo-001";

        // like Map.put()
        client.hset(key, "id", "J001");
        client.hset(key, "name", "John");
        client.hset(key, "gender", "male");
        client.hset(key, "age", "35");
        client.hset(key, "salary", "1000000");

        LOGGER.info("length of 1000000 is " + client.hstrlen(key, "salary"));

        // like Map.get()
        String id = client.hget(key, "id");
        String name = client.hget(key, "name");
        LOGGER.info("User " + id + "'s name is " + name);

        Map<String, String> user = client.hgetAll(key);
        LOGGER.info(user);
        client.del(key);

        key = "userinfo-002";
        Map<String, String> user2 = new HashMap<String, String>();
        user2.put("id", "L002");
        user2.put("name", "Lucy");
        user2.put("gender", "female");
        user2.put("age", "25");
        user2.put("salary", "200000");
        client.hmset(key, user2);
        client.hincrBy(key, "salary", 50000);
        id = client.hget(key, "id");
        String salary = client.hget(key, "salary");
        LOGGER.info("User " + id + "'s salary is " + salary);

        // like Map.keySet()
        Set<String> keys = client.hkeys(key);
        LOGGER.info("all fields: " + keys);
        // like Map.values()
        List<String> values = client.hvals(key);
        LOGGER.info("all values: " + values);

        // Fetch some fields
        values = client.hmget(key, "id", "name");
        LOGGER.info("partial field values: " + values);

        // like Map.containsKey();
        boolean exist = client.hexists(key, "gender");
        LOGGER.info("Exist field gender? " + exist);

        // like Map.remove();
        client.hdel(key, "age");
        keys = client.hkeys(key);
        LOGGER.info("after del field age, rest fields: " + keys);

        client.del(key);

        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value1");
        client.hset(key, map);
        LOGGER.info("key1's value is : " + client.hget(key, "key1"));

        client.unlink(key);
        if (client.get(key) == null) {
            LOGGER.info("Can delete key by UNLINK.");
        }
    }

    /**
     * set 类型测试
     */
    public void testSet() {
        String key = "sets";

        client.sadd(key, "HashSet");
        client.sadd(key, "SortedSet");
        client.sadd(key, "TreeSet");

        // like Set.size()
        long size = client.scard(key);
        LOGGER.info("Set size: " + size);

        client.sadd(key, "SortedSet");
        size = client.scard(key);
        LOGGER.info("Set size: " + size);

        Set<String> sets = client.smembers(key);
        LOGGER.info("Set: " + sets);

        client.srem(key, "SortedSet");
        sets = client.smembers(key);
        LOGGER.info("Set: " + sets);

        boolean ismember = client.sismember(key, "TreeSet");
        LOGGER.info("TreeSet is set's member: " + ismember);

        client.del(key);
    }

    /**
     * sortedset 类型测试
     */
    public void testSortedSet() {
        String key = "hackers";

        // Score: age
        client.zadd(key, 1940, "Alan Kay");
        client.zadd(key, 1953, "Richard Stallman");
        client.zadd(key, 1965, "Yukihiro Matsumoto");
        client.zadd(key, 1916, "Claude Shannon");
        client.zadd(key, 1969, "Linus Torvalds");
        client.zadd(key, 1912, "Alan Turing");

        // sort by score, ascending order
        Set<String> setValues = client.zrange(key, 0, -1);
        LOGGER.info("All hackers: " + setValues);

        long size = client.zcard(key);
        LOGGER.info("Size: " + size);

        Double score = client.zscore(key, "Linus Torvalds");
        LOGGER.info("Score: " + score);

        long count = client.zcount(key, 1960, 1969);
        LOGGER.info("Count: " + count);

        // sort by score, descending order
        Set<String> setValues2 = client.zrevrange(key, 0, -1);
        LOGGER.info("All hackers 2: " + setValues2);

        client.zrem(key, "Linus Torvalds");
        setValues = client.zrange(key, 0, -1);
        LOGGER.info("All hackers: " + setValues);

        client.del(key);
    }

    /**
     * sort list 测试
     */
    public void testKey() {
        String key = "test-key";

        client.set(key, "test");
        client.expire(key, 5);
        long ttl = client.ttl(key);
        LOGGER.info("TTL: " + ttl);

        String type = client.type(key);
        // KEY type may be string, list, hash, set, zset
        LOGGER.info("KEY type: " + type);

        client.del(key);
        client.rpush(key, "1");
        client.rpush(key, "4");
        client.rpush(key, "6");
        client.rpush(key, "3");
        client.rpush(key, "8");
        List<String> result = client.lrange(key, 0, -1);
        LOGGER.info("List: " + result);

        result = client.sort(key);
        LOGGER.info("Sort list: " + result);

        client.del(key);
    }

    /**
     * 序列化测试
     */
    public void testSerialization() {
        client.set("key1", "value1");
        byte[] dump = client.dump("key1");
        client.del("key1");

        client.restore("key1", 1000, dump);
        if (null != client.get("key1")) {
            LOGGER.info("Byte conversion can be done by dump and restore");
        }

        client.del("key1");
    }

    /**
     * pipeline 测试
     */
    public void testPipeline() {
        // Lazy load
        try {
            if (null == pipeline) {
                pipeline = client.getPipeline();
            }

            pipeline.hset("website", "google", "google");
            pipeline.hset("website", "baidu", "baidu");
            pipeline.hset("website", "sina", "sina");

            Map<String, String> map = new HashMap<String, String>();
            map.put("cardid", "card976");
            map.put("username", "jzkangta");
            pipeline.hmset("hash", map);

            // submit
            pipeline.sync();

            pipeline.hget("website", "google");
            pipeline.hget("website", "baidu");
            pipeline.hget("website", "sina");

            // submit and get all return result
            List<Object> result = pipeline.syncAndReturnAll();
            LOGGER.info("Result: " + result);

            client.del("website");
            client.del("hash");
        } catch (Exception e) {
            LOGGER.error("Fail to excute example cluster Pipeline ", e);
        }
    }

    private static String getResource(String name) {
        ClassLoader cl = RedisTest.class.getClassLoader();
        if (cl == null) {
            return null;
        }
        URL url = cl.getResource(name);
        if (url == null) {
            return null;
        }

        try {
            return URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    /**
     * 初始化操作
     *
     * @throws IOException IOException
     */
    public static void init() throws IOException {
        System.setProperty("redis.authentication.jaas", "false");

        if (System.getProperty("redis.authentication.jaas", "false").equals("true")) {
            LoginUtil.setKrb5Config(getResource("config/krb5.conf"));
            principal = "admintest@" + KerberosUtil.getKrb5DomainRealm();
            LoginUtil.setJaasFile(principal, getResource("config/user.keytab"));
            // System.setProperty("SERVER_REALM","hadoop.com");
        }
    }

    /**
     * 入口方法
     *
     * @param args args
     */
    public static void main(String[] args) throws Exception {
        try {
            init();
        } catch (IOException e) {
            LOGGER.error("Failed to init security configuration", e);
            return;
        }

        RedisTest test = new RedisTest();

        test.testString();
        test.testList();
        test.testHash();
        test.testSet();
        test.testSortedSet();
        test.testKey();
        test.testPipeline();
        test.testGeo();
        test.testSerialization();
        test.destory();
    }
}
