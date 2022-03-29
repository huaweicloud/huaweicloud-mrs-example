package com.huawei.bigdata.kafka.example;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.bigdata.kafka.example.security.LoginUtil;

public class Producer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
    
    private final KafkaProducer<String, String> producer;
    
    private final String topic;
    
    private final Boolean isAsync;
    

    // Broker地址列表
    private final static String BOOTSTRAP_SERVER = "bootstrap.servers";
    
    // 客户端ID
    private final static String CLIENT_ID = "client.id";
    
    // Key序列化类
    private final static String KEY_SERIALIZER = "key.serializer";
    
    // Value序列化类
    private final static String VALUE_SERIALIZER = "value.serializer";
    
    // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    private final static String SECURITY_PROTOCOL = "security.protocol";
    
    // 服务名
    private final static String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
    
    // 域名
    private final static String KERBEROS_DOMAIN_NAME = "kerberos.domain.name";

    // 分区类名
    private final static String PARTITIONER_NAME = "partitioner.class";

    // 默认发送100条消息
    private final static int MESSAGE_NUM = 100;
    
    /**
    * 用户自己申请的机机账号keytab文件名称
    */
    private static final String USER_KEYTAB_FILE = "请修改为真实keytab文件名";

    /**
    * 用户自己申请的机机账号名称
    */
    private static final String USER_PRINCIPAL = "请修改为真实用户名称";

    /**
     * Producer constructor
     *
     * @param topicName Topic名称
     * @param asyncEnable 是否异步模式发送
     */
    public Producer(String topicName, Boolean asyncEnable) {
        Properties props = initProperties();
        producer = new KafkaProducer<String, String>(props);
        topic = topicName;
        isAsync = asyncEnable;
    }

    public static Properties initProperties() {
        Properties props = new Properties();
        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        // Broker地址列表
        props.put(BOOTSTRAP_SERVER, kafkaProc.getValues(BOOTSTRAP_SERVER, "localhost:21007"));
        // 客户端ID
        props.put(CLIENT_ID, kafkaProc.getValues(CLIENT_ID, "DemoProducer"));
        // Key序列化类
        props.put(KEY_SERIALIZER,
                kafkaProc.getValues(KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"));
        // Value序列化类
        props.put(VALUE_SERIALIZER,
                kafkaProc.getValues(VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"));
        // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
        props.put(SECURITY_PROTOCOL, kafkaProc.getValues(SECURITY_PROTOCOL, "SASL_PLAINTEXT"));
        // 服务名
        props.put(SASL_KERBEROS_SERVICE_NAME, "kafka");
        // 域名
        props.put(KERBEROS_DOMAIN_NAME, kafkaProc.getValues(KERBEROS_DOMAIN_NAME, "hadoop.hadoop.com"));
        // 分区类名
        props.put(PARTITIONER_NAME,
                kafkaProc.getValues(PARTITIONER_NAME, "com.huawei.bigdata.kafka.example.SimplePartitioner"));
        return props;
    }

    /**
     * 生产者线程执行函数，循环发送消息。
     */
    public void run() {
        LOG.info("New Producer: start.");
        int messageNo = 1;
        // 指定发送多少条消息后sleep1秒
        int intervalMessages = 10;
        
        while (messageNo <= MESSAGE_NUM) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            
            // 构造消息记录
            String key = String.valueOf(messageNo);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, messageStr);
            
            if (isAsync) {
                // 异步发送
                producer.send(record, new DemoCallBack(startTime, messageNo, messageStr));
            } else {
                try {
                    // 同步发送
                    producer.send(record).get();
                } catch (InterruptedException ie) {
                    LOG.info("The InterruptedException occured : {}.", ie);
                } catch (ExecutionException ee) {
                    LOG.info("The ExecutionException occured : {}.", ee);
                }
            }
            messageNo++;
            
            if (messageNo % intervalMessages == 0) {
                // 每发送intervalMessage条消息sleep1秒
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                LOG.info("The Producer have send {} messages.", messageNo);
            }
        }
        
    }
    
    public static void main(String[] args) {
        if (LoginUtil.isSecurityModel()) {
            try {
                LOG.info("Securitymode start.");
                
                // !!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                LoginUtil.securityPrepare(USER_PRINCIPAL, USER_KEYTAB_FILE);
            } catch (IOException e) {
                LOG.error("Security prepare failure.");
                LOG.error("The IOException occured.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        // 是否使用异步发送模式
        final boolean asyncEnable = false;
        Producer producerThread = new Producer(KafkaProperties.TOPIC, asyncEnable);
        producerThread.start();
    }

    class DemoCallBack implements Callback {
        private final Logger logger = LoggerFactory.getLogger(DemoCallBack.class);

        private long startTime;

        private int key;

        private String message;

        public DemoCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * 回调函数，用于处理异步发送模式下，消息发送到服务端后的处理。
         *
         * @param metadata  元数据信息
         * @param exception 发送异常。如果没有错误发生则为Null。
         */
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                logger.info("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), "
                        + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            } else if (exception != null) {
                logger.error("The Exception occured.", exception);
            }
        }
    }
}