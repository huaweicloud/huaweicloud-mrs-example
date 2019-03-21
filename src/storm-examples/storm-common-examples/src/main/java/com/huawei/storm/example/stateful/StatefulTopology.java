package com.huawei.storm.example.stateful;

import com.huawei.storm.example.common.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 本示例简单演示了{@link org.apache.storm.topology.IStatefulBolt}接口的使用
 * 默认情况下state使用'InMemoryKeyValueState'，即在内存中缓存state信息，这种模式无法在重启后继续统计state
 * 当前还支持在Redis上持久化state，如果使用Redis，则需要修改本示例中的redis相关配置项，并且参照Storm-Redis开发指引完成业务打包并提交运行
 */
public class StatefulTopology {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulTopology.class);
    /**
     *要连接的redis服务的ip地址，使用时请修改成正确的ip
     */
    private static final String REDIS_HOST = "192.168.1.1";

    /**
     *要连接的redis服务的端口号，使用时请修改成正确的端口号
     */
    private static final int REDIS_PORT = 22400;

    /**
     *Redis的key值序列化类，用于在Kryo中注册该class并完成序列化。
     */
    private static final String REDIS_KEY_CLASS = null;

    /**
     *Redis的value值序列化类，用于在Kryo中注册该class并完成序列化
     */
    private static final String REDIS_VALUE_CLASS = null;

    /**
     *Redis的key值序列化类，可以自定义实现，配置为null时，使用默认序列化插件org.apache.storm.state.DefaultStateSerializer，配置示例："\"org.apache.storm.state.DefaultStateSerializer\""
     */
    private static final String REDIS_KEY_SERIALIZER_CLASS = null;

    /**
     *Redis的value值序列化类，可以自定义实现，配置为null时，使用默认序列化插件org.apache.storm.state.DefaultStateSerializer，配置示例："\"org.apache.storm.state.DefaultStateSerializer\""
     */
    private static final String REDIS_VALUE_SERIALIZER_CLASS = null;

    /**
     * State的存储介质，当前只支持MEMOER和REDIS两种
     */
    private static final StateType STATE_TYPE = StateType.REDIS;

    /**
     * Redis是否开启安全
     */
    private static final Boolean IS_REDIS_SECURITY_ENABLE = true;

    /**
     * State的存储介质
     */
    private enum StateType {
        MEMORY,
        REDIS
    }

    /**
     * A bolt that uses {@link KeyValueState} to save its state.
     */
    private static class StatefulSumBolt extends BaseStatefulBolt<KeyValueState<String, Long>> {
        String name;
        KeyValueState<String, Long> kvState;
        long sum;
        private OutputCollector collector;

        StatefulSumBolt(String name) {
            this.name = name;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            sum += ((Number) input.getValueByField("value")).longValue();
            LOG.debug("{} sum = {}", name, sum);
            kvState.put("sum", sum);
            collector.emit(input, new Values(sum));
            collector.ack(input);
        }

        @Override
        public void initState(KeyValueState<String, Long> state) {
            kvState = state;
            sum = kvState.get("sum", 0L);
            LOG.debug("Initstate, sum from saved state = {} ", sum);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }
    }

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple);
            LOG.debug("Got tuple {}", tuple);
            collector.emit(tuple.getValues());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            ofd.declare(new Fields("value"));
        }

    }

    public static void setRedisConfig(Config conf) {
        conf.put(Config.TOPOLOGY_STATE_PROVIDER, "org.apache.storm.redis.state.RedisKeyValueStateProvider");
        conf.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG, " {\n" +
                "   \"keyClass\": " + REDIS_KEY_CLASS + ",\n" +
                "   \"valueClass\": " + REDIS_VALUE_CLASS + ",\n" +
                "   \"keySerializerClass\": " + REDIS_KEY_SERIALIZER_CLASS + ",\n" +
                "   \"valueSerializerClass\": " + REDIS_VALUE_SERIALIZER_CLASS + ",\n" +
                "   \"jedisPoolConfig\": {\n" +
                "     \"host\": \"" + REDIS_HOST + "\",\n" +
                "     \"port\": " + REDIS_PORT + ",\n" +
                "     \"timeout\": 2000,\n" +
                "     \"database\": 0,\n" +
                "     \"password\": null\n" +
                "     }\n" +
                " }");

        if(IS_REDIS_SECURITY_ENABLE)
        {
            //增加kerberos认证所需的plugin到列表中，安全模式必选
            List<String> auto_tgts = new ArrayList<String>();
            //keytab方式
            auto_tgts.add("org.apache.storm.security.auth.kerberos.AutoTGTFromKeytab");
            conf.put(Config.TOPOLOGY_AUTO_CREDENTIALS, auto_tgts);
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomIntegerSpout());
        builder.setBolt("partialsum", new StatefulSumBolt("partial"), 1).shuffleGrouping("spout");
        builder.setBolt("printer", new PrinterBolt(), 2).shuffleGrouping("partialsum");
        builder.setBolt("total", new StatefulSumBolt("total"), 1).shuffleGrouping("printer");
        Config conf = new Config();
        conf.setDebug(false);

        if(STATE_TYPE == StateType.REDIS)
        {
            setRedisConfig(conf);
        }

        if(args.length >= 2)
        {
            //用户更改了默认的keytab文件名，这里需要将新的keytab文件名通过参数传入
            conf.put(Config.TOPOLOGY_KEYTAB_FILE, args[1]);
        }

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();
            cluster.submitTopology("test", conf, topology);
            Utils.sleep(40000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
