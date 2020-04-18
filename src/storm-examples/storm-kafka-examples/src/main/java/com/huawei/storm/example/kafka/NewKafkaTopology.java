package com.huawei.storm.example.kafka;

import com.huawei.storm.example.common.SplitSentenceBolt;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;

/**
 * 实现kafkaConsumer->split->count->kafkaProducer逻辑的示例拓扑
 * 本例中kafkaConsumer和kafkaProducer使用kafka 0.9.0的new consumer和new producer
 * 本例中KAFKA_BROKER_LIST配置项必须根据具体情况手动配置，其余配置项可选择性配置
 */
public class NewKafkaTopology {
    
    //使用的流名称
    private static final String[] STREAMS = new String[] { "test_stream" };
    //kafkaSpout可消费的topic列表，可根据具体情况指定多个
    private static final String[] INPUT_TOPICS = new String[] { "input" };
    //kafkaBolt向kakfa写入数据的topic，只能指定一个
    private static final String OUTPUT_TOPIC = "output";

    /**
     * ========================================================== 
     * kafka new consumer/producer 参数名称
     * ==========================================================
     */
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
    private static final String GROUP_ID = "group.id";

    // kafka new consumer/producer 序列化/反序列化参数
    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String KERBEROS_DOMAIN_NAME = "kerberos.domain.name";

    /**
     * ================================================================
     * kafka new consumer.producer 配置默认值
     * ================================================================
     */
    //kerberos服务的domain，请根据集群的真实域名进行配置，命名规则： hadoop.toLowerCase(${default_realm})，比如当前域名为HADOOP.COM，那么domain就应该为hadoop.hadoop.com
    private static final String DEFAULT_KERBEROS_DOMAIN_NAME = "hadoop.hadoop.com";
    
    // group.id名称，任意指定
    private static final String DEFAULT_GROUP_ID = "kafkaSpoutTestGroup";

    // kafka服务名称，不能修改
    private static final String DEFAULT_SERVICE_NAME = "kafka";

    // kafka安全认证协议，当前支持'SASL_PLAINTEXT'和'PLAINTEXT'两种
    private static final String DEFAULT_SECURITY_PROTOCOL = "SASL_PLAINTEXT";

    // 默认序列化/反序列化类
    private static final String DEFAULT_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    // kafka broker信息，其中port由DEFAULT_SECURITY_PROTOCOL类型决定，SASL_PLAINTEXT协议下port为21007，PLAINTEXT协议下port为9092
    private static final String KAFKA_BROKER_LIST = "ip1:port,ip2:port,ip3:port";

    /**
     * ======================================================== 
     * kafkaSpout.KafkaSpoutConfig 配置
     * ========================================================
     */
    // 默认offset提交周期，单位：毫秒
    private static final int DEFAULT_OFFSET_COMMIT_PERIOD_MS = 10000;
    // 默认最大容许的未提交的offset数，若kafkaSpout内部的uncommit_offset数大于该值，则kafkaSpout会暂停消费，直到uncommit_offset小于该值
    private static final int DEFAULT_MAX_UNCOMMIT_OFFSET_NUM = 5000;
    /*
     * kafkaSpout首次执行poll操作时选择offset的策略，当前提供四种策略模式 
     * EARLIEST： 从最开始的offset开始
     * LATEST： 从最后的offset开始
     * UNCOMMITTED_EARLIEST： 从最后提交的offset开始，如果没有offset被提交，则等同于EARLIEST 
     * UNCOMMITTED_LATEST： 从最后提交的offset开始，如果没有offset被提交，则等同于LATEST
     */
    private static final KafkaSpoutConfig.FirstPollOffsetStrategy DEFAULT_STRATEGY = UNCOMMITTED_EARLIEST;

    /**
     * ==========================================================
     * kafkaSpout.retryService 配置
     * ==========================================================
     */
    //第一次重试的初始延迟，单位：微秒（1毫秒=1000微秒）
    private static final int DEFAULT_DELAY = 500;
    //延迟周期，单位: 毫秒
    private static final int DEFAULT_DELAY_PERIOD = 2;
    //默认最大重试次数
    private static final int DEFAULT_MAX_RETRY_TIMES = Integer.MAX_VALUE;
    //最大延迟时间，单位：秒
    private static final int DEFAULT_MAX_DELAY = 10;

    /**
     * ============================================================
     * storm安全认证插件
     * ============================================================
     */
    private static final String SECURITY_AUTO_KEYTAB_PLUGIN = "org.apache.storm.security.auth.kerberos.AutoTGTFromKeytab";

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void main(String[] args) throws Exception {

        // 设置拓扑配置
        Config conf = new Config();

        // 配置安全插件
        setSecurityPlugin(conf);

        if (args.length >= 2) {
            // 用户更改了默认的keytab文件名，这里需要将新的keytab文件名通过参数传入
            conf.put(Config.TOPOLOGY_KEYTAB_FILE, args[1]);
        }

        // 定义KafkaSpout
        KafkaSpout kafkaSpout = new KafkaSpout<String, String>(getKafkaSpoutConfig());

        // CountBolt
        CountBolt countBolt = new CountBolt();
        //SplitBolt
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();

        // KafkaBolt配置信息
        KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>();
        kafkaBolt.withTopicSelector(new DefaultTopicSelector(OUTPUT_TOPIC))
                .withTupleToKafkaMapper(
                        new FieldNameBasedTupleToKafkaMapper("word", "count"));
        kafkaBolt.withProducerProperties(getKafkaProducerProps());

        // 定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout, 10);
        builder.setBolt("split-bolt", splitBolt,10).shuffleGrouping("kafka-spout", STREAMS[0]);
        builder.setBolt("count-bolt", countBolt, 10).fieldsGrouping(
                "split-bolt", new Fields("word"));
        builder.setBolt("kafka-bolt", kafkaBolt, 10).shuffleGrouping("count-bolt");

        // 命令行提交拓扑
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

    private static void setSecurityPlugin(Config conf) {
        // 增加kerberos认证所需的plugin到列表中，安全模式必选
        List<String> auto_tgts = new ArrayList<String>();
        // 当前只支持keytab方式
        auto_tgts.add(SECURITY_AUTO_KEYTAB_PLUGIN);
        // 将端配置的plugin列表写入config指定项中，安全模式必配
        conf.put(Config.TOPOLOGY_AUTO_CREDENTIALS, auto_tgts);
    }

    private static Func<ConsumerRecord<String, String>, List<Object>> TopicTupleBuilder = new TopicTupleBuilder();
    private static class TopicTupleBuilder implements Func<ConsumerRecord<String, String>, List<Object>>, Serializable {
        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.value(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key());
        }
    }

    private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig() {

        Fields outputFields = new Fields("value", "topic", "partition", "offset", "key");
        return new KafkaSpoutConfig.Builder<String, String>(KAFKA_BROKER_LIST, INPUT_TOPICS[0])
                .setProp(getKafkaConsumerProps())
                .setOffsetCommitPeriodMs(DEFAULT_OFFSET_COMMIT_PERIOD_MS)
                .setFirstPollOffsetStrategy(DEFAULT_STRATEGY)
                .setMaxUncommittedOffsets(DEFAULT_MAX_UNCOMMIT_OFFSET_NUM)
                .setRetry(getRetryService())
                .setRecordTranslator(TopicTupleBuilder ,outputFields, STREAMS[0])
                .build();
    }

    private static Map<String, Object> getKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(GROUP_ID, DEFAULT_GROUP_ID);
        props.put(SASL_KERBEROS_SERVICE_NAME, DEFAULT_SERVICE_NAME);
        props.put(SECURITY_PROTOCOL, DEFAULT_SECURITY_PROTOCOL);
        props.put(KEY_DESERIALIZER, DEFAULT_DESERIALIZER);
        props.put(VALUE_DESERIALIZER, DEFAULT_DESERIALIZER);
        props.put(KERBEROS_DOMAIN_NAME, DEFAULT_KERBEROS_DOMAIN_NAME);
        return props;
    }

    private static Properties getKafkaProducerProps() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, KAFKA_BROKER_LIST);
        props.put(SECURITY_PROTOCOL, DEFAULT_SECURITY_PROTOCOL);
        props.put(KEY_SERIALIZER, DEFAULT_SERIALIZER);
        props.put(VALUE_SERIALIZER, DEFAULT_SERIALIZER);
        props.put(SASL_KERBEROS_SERVICE_NAME, DEFAULT_SERVICE_NAME);
        props.put(KERBEROS_DOMAIN_NAME, DEFAULT_KERBEROS_DOMAIN_NAME);
        return props;
    }

    /**
     * 构造KafkaSpoutRetryService，用于管理并重试发送失败的tuples
     * @return
     */
    private static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(getTimeInterval(
                DEFAULT_DELAY, TimeUnit.MICROSECONDS),
                TimeInterval.milliSeconds(DEFAULT_DELAY_PERIOD),
                DEFAULT_MAX_RETRY_TIMES,
                TimeInterval.seconds(DEFAULT_MAX_DELAY));
    }

    private static TimeInterval getTimeInterval(long delay, TimeUnit timeUnit) {
        return new TimeInterval(delay, timeUnit);
    }
}
