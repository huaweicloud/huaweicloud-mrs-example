package com.huawei.storm.example.kafka;

import java.util.Properties;
import java.util.UUID;

import com.huawei.storm.example.common.SplitSentenceBolt;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class SimpleKafkaTopology
{
    
    private static final String KAFKA_SPOUT = "KAFKA_SPOUT";
    
    private static final String SPLIT_BOLT = "SPLIT_BOLT";
    
    private static final String COUNT_BOLT = "COUNT_BOLT";
    
    private static final String KAFKA_BOLT = "KAFKA_BOLT";

    /**
     * kafka broker信息，格式为： ip:port，需要根据集群实际情况修改
     */
    private static final String KAFKA_BROKER_LIST = "请配置broker-ip:broker-port";

    /**
     * zookeeper 服务端信息，格式为： ip:port，需要根据集群实际情况修改
     */
    private static final String KAFKA_ZKSERVER_LIST = "请配置zk-ip:zk-port";
    
    @SuppressWarnings ({"unchecked", "rawtypes"})
    public static void main(String[] args) throws Exception
    {
        //KafkaSpout配置信息
        String inputTopicName = "input";
        String zkServers = KAFKA_ZKSERVER_LIST;//
        String kafkaRoot = "/kafka";
        String connectString = zkServers + kafkaRoot;
        String zkRoot = kafkaRoot + "/" + inputTopicName;
        String appId = UUID.randomUUID().toString();
        
        //KafkaBolt配置信息
        String outputTopicName = "output";
        String brokerlist = KAFKA_BROKER_LIST;

        //构造KafkaSpout对象
        BrokerHosts hosts = new ZkHosts(connectString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, inputTopicName, zkRoot, appId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        CountBolt countBolt = new CountBolt();
        
        //构造KafkaBolt对象
        KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>();

        kafkaBolt.withTopicSelector(new DefaultTopicSelector(outputTopicName)).withTupleToKafkaMapper(
            new FieldNameBasedTupleToKafkaMapper("words", "count"));

        Properties props = new Properties();
        props.put("bootstrap.servers", brokerlist);
        props.put("producer.type", "async");
        props.put("request.required.acks", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaBolt.withProducerProperties(props);
        
        // 构造拓扑，kafkaspout ==> splitBolt ==> countBolt ==> KafkaBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT, kafkaSpout, 1);
        builder.setBolt(SPLIT_BOLT, splitBolt, 1).localOrShuffleGrouping(KAFKA_SPOUT);
        builder.setBolt(COUNT_BOLT, countBolt, 1).fieldsGrouping(SPLIT_BOLT, new Fields("word"));
        builder.setBolt(KAFKA_BOLT, kafkaBolt, 1).shuffleGrouping(COUNT_BOLT);
        
        //命令行提交拓扑
        StormSubmitter.submitTopology(args[0], new Config(), builder.createTopology());
    }
    
}
