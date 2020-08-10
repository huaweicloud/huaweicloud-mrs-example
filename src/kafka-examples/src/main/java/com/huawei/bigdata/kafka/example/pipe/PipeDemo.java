package com.huawei.bigdata.kafka.example.pipe;

import com.huawei.bigdata.kafka.example.KafkaProperties;
import com.huawei.bigdata.kafka.example.security.SecurityPrepare;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates, using the high-level KStream DSL, how to read data from a source (input) topic and how to
 * write data to a sink (output) topic.
 *
 * In this example, we implement a simple "pipe" program that reads from a source topic "streams-file-input"
 * and writes the data as-is (i.e. unmodified) into a sink topic "streams-pipe-output".
 *
 * Before running this example you must create the input topic and the output topic (e.g. via
 * bin/kafka-topics.sh --create ...), and write some data to the input topic (e.g. via
 * bin/kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
 */
public class PipeDemo {

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = "bootstrap.servers";
        final String securityProtocol = "security.protocol";
        // 服务名
        final String saslKerberosServiceName = "sasl.kerberos.service.name";
        // 域名
        final String kerberosDomainName = "kerberos.domain.name";
        KafkaProperties kafkaProc = KafkaProperties.getInstance();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(bootstrapServers,
                kafkaProc.getValues(bootstrapServers, "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(securityProtocol, kafkaProc.getValues(securityProtocol, "PLAINTEXT"));
        // 服务名
        props.put(saslKerberosServiceName, "kafka");
        // 域名
        props.put(kerberosDomainName, kafkaProc.getValues(kerberosDomainName, "hadoop.hadoop.com"));

        SecurityPrepare.kerbrosLogin();

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("streams-plaintext-input").to("streams-pipe-output");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
