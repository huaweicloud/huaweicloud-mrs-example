package com.huawei.bigdata.flink.examples.cep;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * This class puts new {@link PatternProcessor} to kafka. You need change code and rerun this class
 * for new pattern put.
 */
public class PatternToKafka {
    public static void main(String[] args) {
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        String bootstrapServers = paraTool.get("bootstrap.servers");
        int filterValue = paraTool.getInt("filter");
        String patternTopic = "patterns";

        // Step 1: put pattern to kafka
        putPatternToKafka(bootstrapServers, patternTopic, filterValue);
    }

    private static void putPatternToKafka(String bootstrapServers, String topic, int fliterValue) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers); // Kafka broker address
        props.put("key.serializer", StringSerializer.class.getName()); // Key serializer (will not be used)
        props.put("value.serializer", PatternProcessorSerializer.class.getName()); // Value serializer

        try (KafkaProducer<String, PatternProcessor<RowData>> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, PatternProcessor<RowData>> record =
                    new ProducerRecord<>(topic, null, getPatternProcessor(fliterValue));

            producer.send(
                    record,
                    (metadata, exception) -> {
                        if (exception != null) {
                            exception.printStackTrace();
                        } else {
                            System.out.println(
                                    "Record sent successfully to partition "
                                            + metadata.partition()
                                            + " at offset "
                                            + metadata.offset());
                        }
                    });

            producer.flush();
        }
    }

    private static PatternProcessor<RowData> getPatternProcessor(int filterValue) {
        Pattern<RowData, RowData> pattern =
                Pattern.<RowData>begin("first").where(new SimpleConditionRowData(filterValue));

        PatternProcessFunction<RowData, RowData> patternProcessFunction = new PatternProcessFunctionRowData();

        return new PatternProcessorRowData("PatternProcessor", pattern, patternProcessFunction, 0);
    }
}
