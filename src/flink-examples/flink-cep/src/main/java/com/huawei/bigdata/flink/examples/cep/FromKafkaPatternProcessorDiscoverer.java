package com.huawei.bigdata.flink.examples.cep;

import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.cep.dynamic.processor.PatternProcessorManager;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/* * {@link PatternProcessorDiscoverer} that reads {@link PatternProcessor} from kafka. */
public class FromKafkaPatternProcessorDiscoverer implements PatternProcessorDiscoverer<RowData> {
    private final KafkaConsumer<String, PatternProcessor<RowData>> consumer;

    public FromKafkaPatternProcessorDiscoverer(String bootstrapServers, String groupId, String topic) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", bootstrapServers); // Kafka broker address
        kafkaProps.put("group.id", groupId); // Consumer group id
        kafkaProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Key deserializer
        kafkaProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                PatternProcessorDeserializer.class.getName()); // Value deserializer
        kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        // For earliest regeneration
        //        kafkaProps.put("auto.offset.reset", "earliest");
        //        kafkaProps.put("scan.startup.mode", "earliest-offset");

        consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void discoverPatternProcessorUpdates(PatternProcessorManager<RowData> patternProcessorManager) {
        ConsumerRecords<String, PatternProcessor<RowData>> records =
                consumer.poll(Duration.ofMillis(100)); // Poll with timeout of 100 ms

        if (!records.isEmpty()) {
            List<PatternProcessor<RowData>> recordsList = new ArrayList<>();
            for (ConsumerRecord<String, PatternProcessor<RowData>> record : records) {
                recordsList.add(record.value());
            }

            patternProcessorManager.onPatternProcessorsUpdated(recordsList);
        }
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
