package com.huawei.bigdata.kafka.example;/*
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


import com.huawei.bigdata.kafka.example.security.LoginUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/**
 * Demonstrates, using the low-level Processor APIs, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 * <p>
 * In this example, the input stream reads from a topic named "streams-wordcount-processor-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-processor-output" where each record
 * is an updated count of a single word.
 * <p>
 * Before running this example you must create the input topic and the output topic (e.g. via
 * bin/kafka-topics.sh --create ...), and write some data to the input topic (e.g. via
 * bin/kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
 */
public class WordCountProcessorDemo {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountProcessorDemo.class);

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

    // 处理器拓扑中的Source node name
    private static final String SOURCE_NODE_NAME = "Source";
    // 处理器拓扑中的Process node name
    private static final String PROCESS_NODE_NAME = "Process";
    // 处理器拓扑中的Sink node name
    private static final String SINK_NODE_NAME = "Sink";

    // 正则表达式，本样例代码中使用空白字符(空格、制表符、换页符等)来分割记录
    private final static String REGEX_STRING = "\\s+";
    // 关闭处理程序线程序表示
    private final static String SHUTDOWN_HOOK_THREAD_NAME = "streams-wordcount-shutdown-hook";
    // key-value状态存储名称
    private final static String KEY_VALUE_STATE_STORE_NAME = "Counts";
    // 用户自己创建的source-topic名称，即input topic
    private static final String INPUT_TOPIC_NAME = "streams-wordcount-processor-input";
    // 用户自己创建的sink-topic名称，即output topic
    private static final String OUTPUT_TOPIC_NAME = "streams-wordcount-processor-output";
    // 用户自己申请的机机账号keytab文件名称
    private static final String USER_KEYTAB_FILE = "用户自己申请的机机账号keytab文件名称";
    // 用户自己申请的机机账号名称
    private static final String USER_PRINCIPAL = "用户自己申请的机机账号名称";

    /**
     * 定制Kafka-Streams中的处理器Processor
     */
    private static class MyProcessorSupplier implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                // ProcessorContext实例，它提供对当前正在处理的记录的元数据的访问
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(ProcessorContext context) {
                    // 在本地保留processor context，因为在punctuate()和commit()时会用到
                    this.context = context;
                    // 每1秒执行一次punctuate()
                    this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                        try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
                            System.out.println("----------- " + timestamp + " ----------- ");

                            while (iter.hasNext()) {
                                final KeyValue<String, Integer> entry = iter.next();

                                System.out.println("[" + entry.key + ", " + entry.value + "]");

                                context.forward(entry.key, entry.value.toString());
                            }
                        }
                    });
                    // 检索名称为KEY_VALUE_STATE_STORE_NAME的key-value状态存储区，可用于记忆最近收到的输入记录等
                    this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore(KEY_VALUE_STATE_STORE_NAME);
                }

                // 对input topic的接收记录进行处理，将记录拆分为单词并计数
                @Override
                public void process(String dummy, String line) {
                    String[] words = line.toLowerCase(Locale.getDefault()).split(REGEX_STRING);

                    for (String word : words) {
                        Integer oldValue = this.kvStore.get(word);

                        if (oldValue == null) {
                            this.kvStore.put(word, 1);
                        } else {
                            this.kvStore.put(word, oldValue + 1);
                        }
                    }
                }

                @Override
                public void close() {
                }
            };
        }
    }

    public static void main(String[] args) throws Exception {
        /**
         * 进行安全认证
         */
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

        // Kafka-Streams配置
        Properties props = new Properties();
        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        // Broker地址列表
        props.put(BOOTSTRAP_SERVERS, kafkaProc.getValues(BOOTSTRAP_SERVERS, "localhost:21007"));
        // 服务名
        props.put(SASL_KERBEROS_SERVICE_NAME, "kafka");
        // 域名
        props.put(KERBEROS_DOMAIN_NAME, kafkaProc.getValues(KERBEROS_DOMAIN_NAME, "hadoop.hadoop.com"));
        // !!注意，Stream应用程序比标识符，Kafka集群中必须唯一
        props.put(APPLICATION_ID, kafkaProc.getValues(APPLICATION_ID, "streams-wordcount-processor"));
        // 协议类型：当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
        props.put(SECURITY_PROTOCOL, kafkaProc.getValues(SECURITY_PROTOCOL, "SASL_PLAINTEXT"));
        // 用于缓冲的最大内存字节数
        props.put(CACHE_MAX_BYTES_BUFFERING, 0);
        // 实现Serde接口的消息key值的序列化器/反序列化器
        props.put(DEFAULT_KEY_SERDE, Serdes.String().getClass());
        // 实现Serde接口的消息内容的序列化器/反序列化器
        props.put(DEFAULT_VALUE_SERDE, Serdes.String().getClass());
        // 将偏移复位设置为最早，以便我们可以使用相同的预加载数据重新运行演示代码
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 构造处理器拓扑
        Topology builder = new Topology();
        // 添加source processor node，将source-topic作为输入
        builder.addSource(SOURCE_NODE_NAME, INPUT_TOPIC_NAME);
        // 添加WordCountProcessor node，将source processor node作为上游处理节点
        builder.addProcessor(PROCESS_NODE_NAME, new MyProcessorSupplier(), SOURCE_NODE_NAME);
        // 生成key-value状态存储区，将状态信息和对应的WordCountProcessor node关联起来
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(KEY_VALUE_STATE_STORE_NAME),
                Serdes.String(),
                Serdes.Integer()),
                PROCESS_NODE_NAME);
        // 添加sink processor node，将sink-topic作为输出，WordCountProcessor node作为上游处理节点
        builder.addSink(SINK_NODE_NAME, OUTPUT_TOPIC_NAME, PROCESS_NODE_NAME);

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // 添加关闭处理程序来捕获Ctrl-c
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
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}


