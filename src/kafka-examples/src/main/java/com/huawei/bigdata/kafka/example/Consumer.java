package com.huawei.bigdata.kafka.example;

import com.huawei.bigdata.kafka.example.security.LoginUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private final KafkaConsumer<String, String> consumer;

    private final String topic;

    private volatile boolean closed;

    private final CountDownLatch latch;

    // 一次请求的最大等待时间(S)
    private final int waitTime = 1;

    // Broker连接地址
    private final static String BOOTSTRAP_SERVER = "bootstrap.servers";

    // Group id
    private final static String GROUP_ID = "group.id";

    // 消息内容使用的反序列化类
    private final static String VALUE_DESERIALIZER = "value.deserializer";

    // 消息Key值使用的反序列化类
    private final static String KEY_DESERIALIZER = "key.deserializer";

    // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    private final static String SECURITY_PROTOCOL = "security.protocol";

    // 服务名
    private final static String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";

    // 域名
    private final static String KERBEROS_DOMAIN_NAME = "kerberos.domain.name";

    // 是否自动提交offset
    private final static String ENABLE_AUTO_COMMIT = "enable.auto.commit";

    // 自动提交offset的时间间隔
    private final static String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";

    // 会话超时时间
    private final static String SESSION_TIMEOUT_MS = "session.timeout.ms";

    /**
     * 用户自己申请的机机账号keytab文件名称
     */
    private static final String USER_KEYTAB_FILE = "请修改为真实keytab文件名";

    /**
     * 用户自己申请的机机账号名称
     */
    private static final String USER_PRINCIPAL = "请修改为真实用户名称";

    /**
     * Consumer构造函数
     *
     * @param topic 订阅的Topic名称
     */
    public Consumer(String topic, CountDownLatch latch) {
        super("KafkaConsumerExample");
        Properties props = initProperties();
        consumer = new KafkaConsumer<String, String>(props);
        this.topic = topic;
        this.latch = latch;
        // 订阅
        consumer.subscribe(Collections.singletonList(this.topic));
    }

    public static Properties initProperties() {
        Properties props = new Properties();
        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        // Broker连接地址
        props.put(BOOTSTRAP_SERVER, kafkaProc.getValues(BOOTSTRAP_SERVER, "localhost:21007"));
        // Group id
        props.put(GROUP_ID, kafkaProc.getValues(GROUP_ID, "DemoConsumer"));
        // 是否自动提交offset
        props.put(ENABLE_AUTO_COMMIT, kafkaProc.getValues(ENABLE_AUTO_COMMIT, "true"));
        // 自动提交offset的时间间隔
        props.put(AUTO_COMMIT_INTERVAL_MS, kafkaProc.getValues(AUTO_COMMIT_INTERVAL_MS,"1000"));
        // 会话超时时间
        props.put(SESSION_TIMEOUT_MS, kafkaProc.getValues(SESSION_TIMEOUT_MS, "30000"));
        // 消息Key值使用的反序列化类
        props.put(KEY_DESERIALIZER,
            kafkaProc.getValues(KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer"));
        // 消息内容使用的反序列化类
        props.put(VALUE_DESERIALIZER,
            kafkaProc.getValues(VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer"));
        // 安全协议类型
        props.put(SECURITY_PROTOCOL, kafkaProc.getValues(SECURITY_PROTOCOL, "SASL_PLAINTEXT"));
        // 服务名
        props.put(SASL_KERBEROS_SERVICE_NAME, "kafka");
        // 域名
        props.put(KERBEROS_DOMAIN_NAME, kafkaProc.getValues(KERBEROS_DOMAIN_NAME, "hadoop.hadoop.com"));

        return props;
    }

    /**
     * 订阅Topic的消息处理函数
     */
    public void run() {
        while (!closed) {
            try {
                // 消息消费请求
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(waitTime));
                // 消息处理
                for (ConsumerRecord<String, String> record : records) {
                    LOG.info("[ConsumerExample], Received message: (" + record.key() + ", " + record.value()
                        + ") at offset " + record.offset());
                }
            } catch (AuthorizationException | UnsupportedVersionException
                     | RecordDeserializationException e) {
                LOG.error(e.getMessage());
                // 无法从异常中恢复
                closeThread();
                latchShutDown();
            } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                LOG.error("Invalid or no offset found, using latest");
                consumer.seekToEnd(e.partitions());
                consumer.commitSync();
            } catch (KafkaException e) {
                LOG.error(e.getMessage());
            }
        }
        latchShutDown();
    }

    public void closeThread() {
        if (!closed) {
            closed = true;
        }
    }

    public void latchShutDown() {
        latch.countDown();
    }

    public static void main(String[] args) {
        if (LoginUtil.isSecurityModel()) {
            try {
                LOG.info("Securitymode start.");

                //!!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                LoginUtil.securityPrepare(USER_PRINCIPAL, USER_KEYTAB_FILE);
            } catch (IOException e) {
                LOG.error("Security prepare failure.");
                LOG.error("The IOException occured : {}.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC, new CountDownLatch(1));
        consumerThread.start();

        // 等到60s后将consumer关闭，实际执行过程中可修改
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            LOG.info("The InterruptedException occured : {}.", e);
        } finally {
            consumerThread.closeThread();
            try {
                consumerThread.latch.await();
            } catch (InterruptedException e) {
                LOG.error("consumerThread.latch.await() is error", e);
            }
            consumerThread.consumer.close();
        }
    }

}
