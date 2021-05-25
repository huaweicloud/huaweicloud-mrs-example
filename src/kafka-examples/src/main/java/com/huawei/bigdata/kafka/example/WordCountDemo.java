/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.bigdata.kafka.example;

import com.huawei.bigdata.kafka.example.security.LoginUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 * <p>
 * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
 * is an updated count of a single word.
 * <p>
 * Before running this example you must create the input topic and the output topic (e.g. via
 * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
 * {@code bin/kafka-console-producer.sh}). Otherwise you won't see any data arriving in the output topic.
 */
public final class WordCountDemo {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountDemo.class);

    // Broker地址列表
    private final static String BOOTSTRAP_SERVERS = "bootstrap.servers";
    // 服务名
    private final static String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
    // 域名
    private final static String KERBEROS_DOMAIN_NAME = "kerberos.domain.name";
    // !!注意，Stream应用程序比标识符，Kafka集群中必须唯一
    private final static String APPLICATION_ID = "application.id";
    // 协议类型：当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    private final static String SECURITY_PROTOCOL = "security.protocol";
    // 用于缓冲的最大内存字节数
    private final static String CACHE_MAX_BYTES_BUFFERING = "cache.max.bytes.buffering";
    // 实现Serde接口的消息key值的序列化器/反序列化器
    private final static String DEFAULT_KEY_SERDE = "default.key.serde";
    // 实现Serde接口的消息内容的序列化器/反序列化器
    private final static String DEFAULT_VALUE_SERDE = "default.value.serde";

    /**
     * 用户可配置参数如下
     */
    // 正则表达式，本样例代码中使用空白字符(空格、制表符、换页符等)来分割记录
    private final static String REGEX_STRING = "\\s+";
    // 关闭处理程序线程序表示
    private final static String SHUTDOWN_HOOK_THREAD_NAME = "streams-wordcount-shutdown-hook";
    // key-value状态存储名称
    private final static String KEY_VALUE_STATE_STORE_NAME = "WordCounts";
    // 用户自己创建的source-topic名称，即input topic
    private static final String INPUT_TOPIC_NAME = "streams-wordcount-input";
    // 用户自己创建的sink-topic名称，即output topic
    private static final String OUTPUT_TOPIC_NAME = "streams-wordcount-output";
    // 用户自己申请的机机账号keytab文件名称
    private static final String USER_KEYTAB_FILE = "用户自己申请的机机账号keytab文件名称";
    // 用户自己申请的机机账号名称
    private static final String USER_PRINCIPAL = "用户自己申请的机机账号名称";

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        KafkaProperties kafkaProc = KafkaProperties.getInstance();
        // Broker地址列表
        props.put(BOOTSTRAP_SERVERS, kafkaProc.getValues(BOOTSTRAP_SERVERS, "localhost:21007"));
        // 服务名
        props.put(SASL_KERBEROS_SERVICE_NAME, "kafka");
        // 域名
        props.put(KERBEROS_DOMAIN_NAME, kafkaProc.getValues(KERBEROS_DOMAIN_NAME, "hadoop.hadoop.com"));
        // !!注意，Stream应用程序比标识符，Kafka集群中必须唯一
        props.put(APPLICATION_ID, kafkaProc.getValues(APPLICATION_ID, "streams-wordcount"));
        // 协议类型：当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
        props.put(SECURITY_PROTOCOL, kafkaProc.getValues(SECURITY_PROTOCOL, "SASL_PLAINTEXT"));
        // 用于缓冲的最大内存字节数
        props.put(CACHE_MAX_BYTES_BUFFERING, 0);
        // 实现Serde接口的消息key值的序列化器/反序列化器
        props.put(DEFAULT_KEY_SERDE, Serdes.String().getClass().getName());
        // 实现Serde接口的消息内容的序列化器/反序列化器
        props.put(DEFAULT_VALUE_SERDE, Serdes.String().getClass().getName());

        // 将偏移复位设置为最早，以便我们可以使用相同的预加载数据重新运行演示代码
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void createWordCountStream(final StreamsBuilder builder) {
        // 从 input-topic 接收输入记录
        final KStream<String, String> source = builder.stream(INPUT_TOPIC_NAME);

        // 聚合 key-value 键值对的计算结果
        final KTable<String, Long> counts = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(REGEX_STRING)))
                .groupBy((key, value) -> value)
                .count();

        // 将计算结果的 key-value 键值对从 output topic 输出
        counts.toStream().to(OUTPUT_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public static void main(final String[] args) {
        if (LoginUtil.isSecurityModel()) {
            try {
                LOG.info("Securitymode start.");

                // 注意，安全认证时，需要用户手动修改为自己申请的机机账号
                LoginUtil.securityPrepare(USER_PRINCIPAL, USER_KEYTAB_FILE);
            } catch (IOException e) {
                LOG.error("Security prepare failure.");
                LOG.error("The IOException occured.", e);
                return;
            }
            LOG.info("Security prepare success.");
        }

        final Properties props = getStreamsConfig();

        // 构造处理器拓扑
        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // 添加关闭处理程序来捕获 Ctrl-c
        Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK_THREAD_NAME) {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            // 启动Kafka-Streams
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
