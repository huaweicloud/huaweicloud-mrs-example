package com.huawei.bigdata.kafka.example;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.huawei.bigdata.kafka.example.security.SecurityPrepare;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerMultThread extends Thread
{
    private static Logger LOG = LoggerFactory.getLogger(ProducerMultThread.class);

    // 并发的线程数
    private static final int PRODUCER_THREAD_COUNT = 2;

    private static final Properties props = new Properties();

    private int sendThreadId = 0;

    private final KafkaProducer<Integer, String> producer;

    private final String topic;

    private final Boolean isAsync;

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

    /**
     * ProducerMultThread 构造函数
     * @param topicName Topic名称
     * @param asyncEnable 是否异步模式发送
     * @param threadNum 线程数
     */
    public ProducerMultThread(String topicName, Boolean asyncEnable, int threadNum)
    {
        this.sendThreadId = threadNum;

        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        // Broker地址列表
        props.put(bootstrapServers, kafkaProc.getValues(bootstrapServers, "localhost:9092"));
        // 客户端ID
        props.put(clientId, "DemoProducer"+threadNum);
        // Key序列化类
        props.put(keySerializer,
                kafkaProc.getValues(keySerializer, "org.apache.kafka.common.serialization.IntegerSerializer"));
        // Value序列化类
        props.put(valueSerializer,
                kafkaProc.getValues(valueSerializer, "org.apache.kafka.common.serialization.StringSerializer"));
        // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
        props.put(securityProtocol, kafkaProc.getValues(securityProtocol, "PLAINTEXT"));
        // 服务名
        props.put(saslKerberosServiceName, "kafka");
        // 域名
        props.put(kerberosDomainName, kafkaProc.getValues(kerberosDomainName, "hadoop.hadoop.com"));

        producer = new KafkaProducer<Integer, String>(props);
        topic = topicName;
        isAsync = asyncEnable;
    }

    /**
     * 启动多个线程进行发送
     */
    public void run()
    {
        LOG.info("Producer: " + this.getId() +" start.");
        // 用于记录消息条数
        int messageCount = 1;

        // 每个线程发送的消息条数
        int messagesPerThread = 5;

        while (messageCount <= messagesPerThread)
        {
            // 待发送的消息内容
            String messageStr = new String("Message_" + sendThreadId + "_" + messageCount);

            // 此处对于同一线程指定相同Key值，确保每个线程只向同一个Partition生产消息
            Integer key = new Integer(sendThreadId);

            long startTime = System.currentTimeMillis();

            // 构造消息记录
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, key, messageStr);

            if (isAsync)
            {
                // 异步发送
                producer.send(record, new DemoCallBack(startTime, key, messageStr));
            }
            else
            {
                try
                {
                    // 同步发送
                    Future<RecordMetadata> metadataFuture = producer.send(record);
                    RecordMetadata recordMetadata = metadataFuture.get();
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
            LOG.info("Producer: send " + messageStr + " to " + topic + " with key: " + key);
            messageCount++;

            // 每隔1s，发送1条消息
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }

            producer.flush();
        }
    }

    public static void main(String[] args)
    {
        SecurityPrepare.kerbrosLogin();

        // 是否使用异步发送模式
        final boolean asyncEnable = false;

        // 指定的线程号，仅用于区分不同的线程
        for (int threadNum = 0; threadNum < PRODUCER_THREAD_COUNT; threadNum++)
        {
            ProducerMultThread producerMultThread = new ProducerMultThread(KafkaProperties.TOPIC, asyncEnable, threadNum);
            producerMultThread.start();
        }

    }

    class DemoCallBack implements Callback
    {
        private Logger LOG = LoggerFactory.getLogger(com.huawei.bigdata.kafka.example.DemoCallBack.class);

        private long startTime;

        private int key;

        private String message;

        public DemoCallBack(long startTime, int key, String message)
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

}