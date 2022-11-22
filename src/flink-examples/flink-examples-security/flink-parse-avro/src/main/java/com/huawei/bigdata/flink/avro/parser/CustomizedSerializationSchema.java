package com.huawei.bigdata.flink.avro.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Map;

public class CustomizedSerializationSchema implements KafkaSerializationSchema<Map<String, Object>> {

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Map<String, Object> map, @Nullable Long aLong) {
        ProducerRecord<byte[], byte[]> producerRecord = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            String topic = (String) map.get(Avro2Json.TOPIC);
            String tableName = (String) map.get(Avro2Json.DRS_TABLE_NAME);
            String jsonStr = mapper.writeValueAsString(map);
            producerRecord = new ProducerRecord<>(topic, tableName.getBytes(), jsonStr.getBytes());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return producerRecord;
    }
}
