/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import com.huawei.jredis.client.SslSocketFactoryUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

/**
 * Read stream data and join from configure table from redis.
 * 
 * @since 2019/9/30
 */
public class FlinkConfigtableJavaExample {

    private static final int MAX_ATTEMPTS = 2;

    private static final int TIMEOUT = 60000;

    public static void main(String[] args) throws Exception {
        // print comment for command to use run flink
        System.out.println(
                "use command as: \n"
                    + "./bin/flink run --class com.huawei.bigdata.flink.examples.FlinkConfigtableJavaExample -m"
                    + " yarn-cluster -yt /opt/config -yn 3 -yjm 1024 -ytm 1024 /opt/FlinkConfigtableJavaExample.jar"
                    + " --dataPath"
                    + " config/data.txt******************************************************************************************\n"
                    + "Especially you may write following content into config filePath, as in config/read.properties:"
                    + " \n"
                    + "ReadFields=username,age,company,workLocation,educational,workYear,phone,nativeLocation,school\n"
                    + "Redis_IP_Port=SZV1000064084:22400,SZV1000064082:22400,SZV1000064085:22400\n"
                    + "******************************************************************************************");

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(200);
        env.setParallelism(1);

        // get configure and read data and transform to OriginalRecord
        final String dataPath = ParameterTool.fromArgs(args).get("dataPath", "config/data.txt");
        DataStream<OriginalRecord> originalStream =
                env.readTextFile(dataPath)
                        .map(
                                new MapFunction<String, OriginalRecord>() {
                                    @Override
                                    public OriginalRecord map(String value) throws Exception {
                                        return getRecord(value);
                                    }
                                })
                        .assignTimestampsAndWatermarks(new Record2TimestampExtractor())
                        .disableChaining();

        // read from redis and join to the whole user information
        AsyncFunction<OriginalRecord, UserRecord> function = new AsyncRedisRequest();
        // timeout set to 2 minutes, max parallel request num set to 5, you can modify this to optimize
        DataStream<UserRecord> result = AsyncDataStream.unorderedWait(originalStream, function, 2, TimeUnit.MINUTES, 5);

        // data transform
        result.filter(
                        new FilterFunction<UserRecord>() {
                            @Override
                            public boolean filter(UserRecord value) throws Exception {
                                return value.gender.equals("female");
                            }
                        })
                .keyBy(new UserRecordSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .reduce(
                        new ReduceFunction<UserRecord>() {
                            @Override
                            public UserRecord reduce(UserRecord value1, UserRecord value2) throws Exception {
                                value1.shoppingTime += value2.shoppingTime;
                                return value1;
                            }
                        })
                .filter(
                        new FilterFunction<UserRecord>() {
                            @Override
                            public boolean filter(UserRecord value) throws Exception {
                                return value.shoppingTime > 120;
                            }
                        })
                .print();

        // execute program
        env.execute("FlinkConfigtable java");
    }

    private static class UserRecordSelector implements KeySelector<UserRecord, String> {
        @Override
        public String getKey(UserRecord value) throws Exception {
            return value.name;
        }
    }

    // class to set watermark and timestamp
    private static class Record2TimestampExtractor implements AssignerWithPunctuatedWatermarks<OriginalRecord> {
        // add tag in the data of datastream elements
        @Override
        public long extractTimestamp(OriginalRecord element, long previousTimestamp) {
            return System.currentTimeMillis();
        }

        // give the watermark to trigger the window to execute, and use the value to check if the window elements is
        // ready
        @Override
        public Watermark checkAndGetNextWatermark(OriginalRecord element, long extractedTimestamp) {
            return new Watermark(extractedTimestamp - 1);
        }
    }

    private static OriginalRecord getRecord(String line) {
        String[] elems = line.split(",");
        assert elems.length == 3;
        return new OriginalRecord(elems[0], elems[1], Integer.parseInt(elems[2]));
    }

    /**
     * 原始订单类
     * 
     * @since 2019/9/30
     */
    public static class OriginalRecord {
        private String name;
        private String gender;
        private int shoppingTime;

        public OriginalRecord(String name, String gender, int shoppingTime) {
            this.name = name;
            this.gender = gender;
            this.shoppingTime = shoppingTime;
        }
    }

    /**
     * 用户类
     * 
     * @since 2019/9/30
     */
    public static class UserRecord {
        private String name;
        private int age;
        private String company;
        private String workLocation;
        private String educational;
        private int workYear;
        private String phone;
        private String nativeLocation;
        private String school;
        private String gender;
        private int shoppingTime;

        public UserRecord(
                String name,
                int age,
                String company,
                String workLocation,
                String educational,
                int workYear,
                String phone,
                String nativeLocation,
                String school,
                String gender,
                int shoppingTime) {
            this.name = name;
            this.age = age;
            this.company = company;
            this.workLocation = workLocation;
            this.educational = educational;
            this.workYear = workYear;
            this.phone = phone;
            this.nativeLocation = nativeLocation;
            this.school = school;
            this.gender = gender;
            this.shoppingTime = shoppingTime;
        }

        /**
         * 输入信息转换
         * 
         * @param input_nm 姓名
         * @param input_sx 性别
         * @param input_st 购物时间
         */
        public void setInput(String input_nm, String input_sx, int input_st) {
            name = input_nm;
            gender = input_sx;
            shoppingTime = input_st;
        }

        @Override
        public String toString() {
            return "UserRecord-----name: "
                    + name
                    + "  age: "
                    + age
                    + "  company: "
                    + company
                    + "  workLocation: "
                    + workLocation
                    + "  educational: "
                    + educational
                    + "  workYear: "
                    + workYear
                    + "  phone: "
                    + phone
                    + "  nativeLocation: "
                    + nativeLocation
                    + "  school: "
                    + school
                    + "  gender: "
                    + gender
                    + "  shoppingTime: "
                    + shoppingTime;
        }
    }

    /**
     * 异步IO访问Redis
     * 
     * @since 2019/9/30
     */
    public static class AsyncRedisRequest extends RichAsyncFunction<OriginalRecord, UserRecord> {
        private String fields = "";
        private transient JedisCluster client;
        private LoadingCache<String, UserRecord> cacheRecords;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // init cache builder
            cacheRecords =
                    CacheBuilder.newBuilder()
                            .maximumSize(10000)
                            .expireAfterAccess(7, TimeUnit.DAYS)
                            .build(
                                    new CacheLoader<String, UserRecord>() {
                                        @Override
                                        public UserRecord load(String key) throws Exception {
                                            // load from redis
                                            return loadFromRedis(key);
                                        }
                                    });

            // get configure from config/read.properties, you must put this with commands:
            // ./bin/yarn-session.sh -t config -n 3 -jm 1024 -tm 1024 or
            // ./bin/flink run -m yarn-cluster -yt config -yn 3 -yjm 1024 -ytm 1024 /opt/test.jar
            String configPath = "config/read.properties";
            fields = ParameterTool.fromPropertiesFile(configPath).get("ReadFields");
            final String hostPort = ParameterTool.fromPropertiesFile(configPath).get("Redis_IP_Port");
            final boolean ssl = ParameterTool.fromPropertiesFile(configPath).getBoolean("Redis_ssl_on", false);
            // create jedisCluster client
            Set<HostAndPort> hosts = new HashSet<HostAndPort>();
            for (String node : hostPort.split(",")) {
                HostAndPort hostAndPort = genHostAndPort(node);
                if (hostAndPort == null) {
                    continue;
                }
                hosts.add(hostAndPort);
            }
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            final SSLSocketFactory socketFactory = SslSocketFactoryUtil.createTrustALLSslSocketFactory();

            client = new JedisCluster(hosts, TIMEOUT, TIMEOUT, MAX_ATTEMPTS,
                "","", poolConfig, ssl, socketFactory, null,
                null, null);
            System.out.println("JedisCluster init, getClusterNodes: " + client.getClusterNodes().size());
        }

        private HostAndPort genHostAndPort(String ipAndPort) {
            int lastIdx = ipAndPort.lastIndexOf(":");
            if (lastIdx == -1) {
                return null;
            }
            String ip = ipAndPort.substring(0, lastIdx);
            String port = ipAndPort.substring(lastIdx + 1);
            return new HostAndPort(ip, Integer.parseInt(port));
        }

        @Override
        public void close() throws Exception {
            super.close();

            if (client != null) {
                System.out.println("JedisCluster close!!!");
                client.close();
            }
        }

        /**
         * 加载Redis记录
         * 
         * @param key key
         * @return 用户对象
         * @throws Exception 异常
         */
        public UserRecord loadFromRedis(final String key) throws Exception {
            if (client.getClusterNodes().size() <= 0) {
                System.out.println("JedisCluster init failed, getClusterNodes: " + client.getClusterNodes().size());
            }
            if (!client.exists(key)) {
                System.out.println("test-------cannot find data to key:  " + key);
                return new UserRecord("null", 0, "null", "null", "null", 0, "null", "null", "null", "null", 0);
            } else {
                // get some fields
                List<String> values = client.hmget(key, fields.split(","));
                System.out.println("test-------key: " + key + "  get some fields:  " + values.toString());
                return new UserRecord(
                        values.get(0),
                        Integer.parseInt(values.get(1)),
                        values.get(2),
                        values.get(3),
                        values.get(4),
                        Integer.parseInt(values.get(5)),
                        values.get(6),
                        values.get(7),
                        values.get(8),
                        "null",
                        0);
            }
        }

        @Override
        public void asyncInvoke(final OriginalRecord input, final ResultFuture<UserRecord> resultFuture)
                throws Exception {
            // set key string, if you key is more than one column, build your key string with columns
            String key = input.name;
            UserRecord info = cacheRecords.get(key);
            info.setInput(input.name, input.gender, input.shoppingTime);
            resultFuture.complete(Collections.singletonList(info));
        }
    }
}
