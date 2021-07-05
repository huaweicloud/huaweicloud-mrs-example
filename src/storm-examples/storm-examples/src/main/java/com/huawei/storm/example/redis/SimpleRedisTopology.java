/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.redis;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.redis.bolt.RedisLookupBolt;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisClusterConfig.Builder;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @since 2020-09-30
 */
public class SimpleRedisTopology {
    private static final String REDIS_LOOKUP_BOLT = "REDIS_LOOKUP_BOLT";

    private static final String REDIS_STORE_BOLT = "REDIS_STORE_BOLT";

    private static final String WORD_SPOUT = "WORD_SPOUT";

    // 请配置redis服务端IP
    private static final String HOST1 = "请配置服务端IP";

    private static final String HOST2 = "请配置服务端IP";

    private static final String HOST3 = "请配置服务端IP";

    // redis端口号，可根据实际情况自行修改
    private static final int PORT = 22400;

    public static void main(String[] args) throws Exception {
        // 配置redis集群信息，主机IP和端口号请按照实际情况修改
        Set<InetSocketAddress> nodes = new HashSet<InetSocketAddress>();
        InetSocketAddress node1 = new InetSocketAddress(HOST1, PORT);
        InetSocketAddress node2 = new InetSocketAddress(HOST2, PORT);
        InetSocketAddress node3 = new InetSocketAddress(HOST3, PORT);

        nodes.add(node1);
        nodes.add(node2);
        nodes.add(node3);

        Builder clusterConfigBuilder = new Builder();
        clusterConfigBuilder.setNodes(nodes);
        JedisClusterConfig clusterConfig = clusterConfigBuilder.build();

        Config conf = new Config();

        // spout数据源，产生单词
        WordSpout wordSpout = new WordSpout();

        // Redis查询相关内容，并将计数加一
        RedisLookupMapper lookupMapper = new WordCountRedisLookupMapper();
        RedisLookupBolt lookupBolt = new RedisLookupBolt(clusterConfig, lookupMapper);

        // 将计数结果保存在Redis，方便后续继续使用
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(clusterConfig, storeMapper);

        // 构造拓扑，wordspout ==> lookupBolt ==> storeBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(WORD_SPOUT, wordSpout);
        builder.setBolt(REDIS_LOOKUP_BOLT, lookupBolt, 1).fieldsGrouping(WORD_SPOUT, new Fields("word"));
        builder.setBolt(REDIS_STORE_BOLT, storeBolt, 1).localOrShuffleGrouping(REDIS_LOOKUP_BOLT);

        // 增加kerberos认证所需的plugin到列表中，安全模式必选
        List<String> autoTgts = new ArrayList<String>();
        // keytab方式
        autoTgts.add("org.apache.storm.security.auth.kerberos.AutoTGTFromKeytab");

        // 将客户端配置的plugin列表写入config指定项中
        // 安全模式必配
        // 普通模式不用配置，请注释掉该行
        conf.put(Config.TOPOLOGY_AUTO_CREDENTIALS, autoTgts);

        if (args.length >= 2) {
            // 用户更改了默认的keytab文件名，这里需要将新的keytab文件名通过参数传入
            conf.put(Config.TOPOLOGY_KEYTAB_FILE, args[1]);
        }
        // 提交拓扑
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}
