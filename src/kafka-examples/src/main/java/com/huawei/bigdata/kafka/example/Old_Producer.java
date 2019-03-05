package com.huawei.bigdata.kafka.example;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Old_Producer extends Thread
{
    private static final Logger LOG = LoggerFactory.getLogger(Old_Producer.class);
    
    private final kafka.javaapi.producer.Producer<String, String> producer;
    
    private final String topic;
    
    private final Properties props = new Properties();
    
    private final String producerType = "producer.type";
    
    private final String partitionerClass = "partitioner.class";
    
    private final String serializerClass = "serializer.class";
    
    private final String metadataBrokerList = "metadata.broker.list";
    
    private final String bootstrapServers = "bootstrap.servers";
    
    public Old_Producer(String topicName)
    {
        props.put(producerType, "sync");
        
        // 配置SimplePartitioner为解析key值，返回PartitionId.
        props.put(partitionerClass, "com.huawei.bigdata.kafka.example.SimplePartitioner");
        
        // 序列化类
        props.put(serializerClass, "kafka.serializer.StringEncoder");
        
        // Broker列表
        props.put(metadataBrokerList, KafkaProperties.getInstance().getValues(bootstrapServers, "localhost:9092"));
        
        // 创建生产者对象
        producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(props));
        topic = topicName;
    }
    
    /*
     * 启动执行producer，每秒发送一条消息。
     */
    public void run()
    {
        LOG.info("Old Producer: start.");
        int messageNo = 1;
        
        while (true)
        {
            String messageStr = new String("Message_" + messageNo);
            
            // 指定消息序号作为key值
            String key = String.valueOf(messageNo);
            producer.send(new KeyedMessage<String, String>(topic, key, messageStr));
            LOG.info("Producer: send " + messageStr + " to " + topic);
            messageNo++;
            
            // 每隔1s，发送1条消息
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args)
    {
    	Old_Producer producerThread = new Old_Producer(KafkaProperties.TOPIC);
        producerThread.start();
    }
}