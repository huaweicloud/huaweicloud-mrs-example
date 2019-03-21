package com.huawei.bigdata.kafka.example;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.bigdata.kafka.example.security.SecurityPrepare;

public class Consumer
{
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
    
    private final KafkaConsumer<Integer, String> consumer;
    
    private final String topic;
    
    // 一次请求的最大等待时间
    private final int waitTime = 1000;
    
    // Broker连接地址
    private final String bootstrapServers = "bootstrap.servers";
    // Group id
    private final String groupId = "group.id";
    // 消息内容使用的反序列化类
    private final String valueDeserializer = "value.deserializer";
    // 消息Key值使用的反序列化类
    private final String keyDeserializer = "key.deserializer";
    // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    private final String securityProtocol = "security.protocol";
    // 服务名
    private final String saslKerberosServiceName = "sasl.kerberos.service.name";
    // 域名
    private final String kerberosDomainName = "kerberos.domain.name";
    // 是否自动提交offset
    private final String enableAutoCommit = "enable.auto.commit";
    // 自动提交offset的时间间隔
    private final String autoCommitIntervalMs = "auto.commit.interval.ms";
    
    // 会话超时时间
    private final String sessionTimeoutMs = "session.timeout.ms";

    /**
     * NewConsumer构造函数
     * @param topic 订阅的Topic名称
     */
    public Consumer(String topic)
    {
        Properties props = new Properties();
        
        KafkaProperties kafkaProc = KafkaProperties.getInstance();
        // Broker连接地址
        props.put(bootstrapServers,
            kafkaProc.getValues(bootstrapServers, "localhost:9092"));
        // Group id
        props.put(groupId, "DemoConsumer");
        // 是否自动提交offset
        props.put(enableAutoCommit, "true");
        // 自动提交offset的时间间隔
        props.put(autoCommitIntervalMs, "1000");
        // 会话超时时间
        props.put(sessionTimeoutMs, "30000");
        // 消息Key值使用的反序列化类
        props.put(keyDeserializer,
            "org.apache.kafka.common.serialization.IntegerDeserializer");
        // 消息内容使用的反序列化类
        props.put(valueDeserializer,
            "org.apache.kafka.common.serialization.StringDeserializer");
        // 安全协议类型
        props.put(securityProtocol, kafkaProc.getValues(securityProtocol, "PLAINTEXT"));
        // 服务名
        props.put(saslKerberosServiceName, "kafka");
        // 域名
        props.put(kerberosDomainName, kafkaProc.getValues(kerberosDomainName, "hadoop.hadoop.com"));
        consumer = new KafkaConsumer<Integer, String>(props);
        this.topic = topic;
    }

    public static void main(String[] args)
    {
        SecurityPrepare.kerbrosLogin();

        Consumer KafkaConsumer = new Consumer(KafkaProperties.TOPIC);

        // 订阅
        KafkaConsumer.consumer.subscribe(Collections.singletonList(KafkaConsumer.topic));

        while (true) {
            // 消息消费请求
            ConsumerRecords<Integer, String> records = KafkaConsumer.consumer.poll(KafkaConsumer.waitTime);

            // 消息处理
            for (ConsumerRecord<Integer, String> record : records)
            {
                LOG.info("[NewConsumerExample], Received message: (" + record.key() + ", " + record.value()
                        + ") at offset " + record.offset());
            }

            KafkaConsumer.consumer.commitSync();
        }
    }
}
