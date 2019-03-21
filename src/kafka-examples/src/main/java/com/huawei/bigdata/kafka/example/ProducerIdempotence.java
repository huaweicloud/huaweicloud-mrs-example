package com.huawei.bigdata.kafka.example;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.bigdata.kafka.example.security.SecurityPrepare;

public class ProducerIdempotence
{
    private static final Logger LOG = LoggerFactory.getLogger(ProducerIdempotence.class);
    
    private final KafkaProducer<Long, String> producer;
    
    private final String topic;
    
    private final Boolean isAsync;
    
    private final Properties props = new Properties();
    
    // Broker地址列表
    private final String bootstrapServers = "bootstrap.servers";
    
    // 客户端ID
    private final String clientId = "client.id";
    
    // Key序列化类
    private final String keySerializer = "key.serializer";
    
    // Value序列化类
    private final String valueSerializer = "value.serializer";
    
    // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    private final String securityProtocol = "security.protocol";
    
    // 服务名
    private final String saslKerberosServiceName = "sasl.kerberos.service.name";
    
    // 域名
    private final String kerberosDomainName = "kerberos.domain.name";
    
    //默认发送20条消息
    private final int messageNumToSend = 100;

    /**
     * 新Producer 构造函数
     * @param topicName Topic名称
     * @param asyncEnable 是否异步模式发送
     */
    public ProducerIdempotence(String topicName, Boolean asyncEnable)
    {
        
        KafkaProperties kafkaProc = KafkaProperties.getInstance();
        
        // Broker地址列表
        props.put(bootstrapServers, kafkaProc.getValues(bootstrapServers, "localhost:9092"));
        // 客户端ID
        props.put(clientId, kafkaProc.getValues(clientId, "DemoProducer"));
        // Key序列化类
        props.put(keySerializer,
            kafkaProc.getValues(keySerializer, "org.apache.kafka.common.serialization.LongSerializer"));
        // Value序列化类
        props.put(valueSerializer,
            kafkaProc.getValues(valueSerializer, "org.apache.kafka.common.serialization.StringSerializer"));
        // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
        props.put(securityProtocol, kafkaProc.getValues(securityProtocol, "PLAINTEXT"));
        // 服务名
        props.put(saslKerberosServiceName, "kafka");
        // 域名
        props.put(kerberosDomainName, kafkaProc.getValues(kerberosDomainName, "hadoop.hadoop.com"));

        props.put("acks", "all");
        props.put("retries", 3);
        props.put("enable.idempotence", "true");
        props.put("max.in.flight.requests.per.connection", 1);
        props.put("transactional.id", "my-transactional-id");
        
        producer = new KafkaProducer<Long, String>(props);

        topic = topicName;
        isAsync = asyncEnable;
    }
    
    public static void main(String[] args)
    {
        SecurityPrepare.kerbrosLogin();
        
        // 是否使用异步发送模式
        final boolean asyncEnable = false;
        ProducerIdempotence KafkaProducer = new ProducerIdempotence(KafkaProperties.TOPIC, asyncEnable);

        KafkaProducer.producer.initTransactions();
        /**
         * 生产者线程执行函数，循环发送消息。
         */

        LOG.info("New Producer: start.");
        long messageNo = 1;
        // 指定发送多少条消息后sleep1秒
        int intervalMessages=10;

        KafkaProducer.producer.beginTransaction();
        while (messageNo <= KafkaProducer.messageNumToSend)
        {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();

            // 构造消息记录
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(KafkaProducer.topic, messageNo, messageStr);

            if (KafkaProducer.isAsync)
            {
                // 异步发送
                KafkaProducer.producer.send(record, new DemoCallBackIdempotence(startTime, messageNo, messageStr));
            }
            else
            {
                try
                {
                    // 同步发送
                    Future<RecordMetadata> metadataFuture = KafkaProducer.producer.send(record);
                    RecordMetadata recordMetadata = metadataFuture.get();
                    LOG.info("The Producer messages is successful : {}", recordMetadata);
                }
                catch (InterruptedException ie)
                {
                    LOG.info("The InterruptedException occured : {}.", ie);
                }
                catch (ExecutionException ee)
                {
                    LOG.info("The ExecutionException occured : {}.", ee);
                }
            }
            messageNo++;
        }
        KafkaProducer.producer.flush();
        KafkaProducer.producer.commitTransaction();
        KafkaProducer.producer.close();
    }
}

class DemoCallBackIdempotence implements Callback
{
    private static Logger LOG = LoggerFactory.getLogger(DemoCallBackIdempotence.class);
    
    private long startTime;
    
    private long key;
    
    private String message;
    
    public DemoCallBackIdempotence(long startTime, long key, String message)
    {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }
    
    /**
     * 回调函数，用于处理异步发送模式下，消息发送到服务端后的处理。
     * @param metadata  元数据信息
     * @param exception 发送异常。如果没有错误发生则为Null。
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception)
    {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null)
        {
            LOG.info("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), "
                + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        }
        else if (exception != null)
        {
            LOG.error("The Exception occured.", exception);
        }
        
    }
}