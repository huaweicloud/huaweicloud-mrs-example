package com.huawei.bigdata.kafka.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.bigdata.kafka.example.security.SecurityPrepare;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Old_Consumer extends Thread
{
    private static final Logger LOG = LoggerFactory.getLogger(Old_Consumer.class);

    private ConsumerConnector consumer;
    
    private String topic;
    
    public Old_Consumer(String topic)
    {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic = topic;
    }
    
    /**
     * 创建consumer的Config
     * @return  [ConsumerConfig]
     */
    private static ConsumerConfig createConsumerConfig()
    {
        KafkaProperties kafkaPros = KafkaProperties.getInstance();
        LOG.info("ConsumerConfig: entry.");
        
        Properties props = new Properties();
        
        props.put("zookeeper.connect", 
            kafkaPros.getValues("zookeeper.connect", "localhost:2181"));
        props.put("group.id", kafkaPros.getValues("group.id", "example-group1"));
        props.put("zookeeper.session.timeout.ms",
            kafkaPros.getValues("zookeeper.session.timeout.ms", "15000"));
        props.put("zookeeper.sync.time.ms",
            kafkaPros.getValues("zookeeper.sync.time.ms", "2000"));
        props.put("auto.commit.interval.ms",
            kafkaPros.getValues("auto.commit.interval.ms", "10000"));
        
        LOG.info("ConsumerConfig: props is " + props);
        
        return new ConsumerConfig(props);
    }
    
    public void run()
    {
        LOG.info("Consumer: start.");
        
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        
        LOG.info("Consumerstreams size is : " + streams.size());
        
        for (KafkaStream<byte[], byte[]> stream : streams)
        {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            
            while (it.hasNext())
            {
                LOG.info("Consumer: receive " + new String(it.next().message()) + " from " + topic);
            }
        }
        
        LOG.info("Consumer End.");
    }

    public static void main(String[] args)
    {
        SecurityPrepare.kerbrosLogin();
        
        Old_Consumer consumerThread = new Old_Consumer(KafkaProperties.TOPIC);
        consumerThread.start();
    }

}
