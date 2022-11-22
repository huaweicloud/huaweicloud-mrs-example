package com.huawei.bigdata.flink.avro.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huawei.hwclouds.drs.avro.Record;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomizedDeSerializationSchema implements KafkaDeserializationSchema<Map<String, Object>> {

    @Override
    public boolean isEndOfStream(Map<String, Object> map) {
        return false;
    }

    @Override
    public Map<String, Object> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = new HashMap<>();
        if (consumerRecord != null && consumerRecord.key() != null && consumerRecord.value() != null) {
            try {
                Object rec = Record.getDecoder().decode(consumerRecord.value());
                Map<String, Object> mapAll = mapper.readValue(rec.toString(), new TypeReference<HashMap<String, Object>>(){});
                map.put(Avro2Json.TOPIC, consumerRecord.topic());
                map.put(Avro2Json.DRS_TABLE_NAME, mapAll.get(Avro2Json.DRS_TABLE_NAME));
                map.put(Avro2Json.DRS_UPDATE_TIMESTAMP, mapAll.get(Avro2Json.DRS_UPDATE_TIMESTAMP));
                map.put(Avro2Json.DRS_OPERATION, mapAll.get(Avro2Json.DRS_OPERATION));
                map.put("columnNames", mapAll.get(Avro2Json.DRS_FIELDS));
                map.put("columnValues", mapAll.get(Avro2Json.DRS_AFTER_IMAGES));
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }
        return map;
    }

    @Override
    public TypeInformation<Map<String, Object>> getProducedType() {
        return TypeInformation.of(new TypeHint<Map<String, Object>>(){});
    }
}
