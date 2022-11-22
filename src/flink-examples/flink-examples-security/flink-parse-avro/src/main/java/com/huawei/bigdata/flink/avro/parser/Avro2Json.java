package com.huawei.bigdata.flink.avro.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huawei.hwclouds.drs.avro.Record;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Avro2Json {
    public static final String TOPIC = "topic";

    public static final String DRS_VERSION = "version";
    public static final String DRS_SEQ_NO = "seqno";
    public static final String DRS_SHARD_ID = "shardId";
    public static final String DRS_EVENT_ID = "eventid";
    public static final String DRS_UPDATE_TIMESTAMP = "updateTimestamp";
    public static final String DRS_TABLE_NAME = "tableName";
    public static final String DRS_OPERATION = "operation";
    public static final String DRS_COLUMN_COUNT = "columnCount";
    public static final String DRS_FIELDS = "fields";
    public static final String DRS_BEFORE_IMAGES = "beforeImages";
    public static final String DRS_AFTER_IMAGES = "afterImages";

    public static final String DEBEZIUM_TS_MS = "ts_ms";
    public static final String DEBEZIUM_OP = "op";
    public static final String DEBEZIUM_SOURCE = "source";
    public static final String DEBEZIUM_BEFORE = "before";
    public static final String DEBEZIUM_AFTER = "after";
    public static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        ParameterTool paraTool = ParameterTool.fromArgs(args);
        Properties props = paraTool.getProperties();
        String avroTopic = props.getProperty("avro_topic", "default_avro_topic");
        String jsonTopic = props.getProperty("json_topic", "default_json_topic");
        int parallelism = Integer.parseInt(props.getProperty("partition_num", "1"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataStream<String> messageStream =
                env.addSource(
                        new FlinkKafkaConsumer<>(
                                avroTopic, new KafkaDeserializationSchema<String>() {
                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
                                String str = "";
                                    try {
                                        if (consumerRecord != null && consumerRecord.key() != null && consumerRecord.value() != null) {
                                            Object rec = Record.getDecoder().decode(consumerRecord.value());
                                            str = rec.toString();
                                        }
                                    } catch (IOException exception) {
                                        exception.printStackTrace();
                                    }
                                return str;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(new TypeHint<String>(){});
                            }
                        }, props));

//        messageStream.filter(s -> !s.isEmpty()).map(new MapFunction<String, Map<String, Object>>() {
//            @Override
//            public Map<String, Object> map(String jsonStr) throws Exception {
//                Map<String, Object> map = new HashMap<>();
//                Map<String, Object> mapAll = mapper.readValue(jsonStr, new TypeReference<HashMap<String, Object>>() {
//                });
//                map.put(TABLE_NAME, StringUtils.remove((String) mapAll.get(TABLE_NAME), '\"'));
//                map.put(UPDATE_TIMESTAMP, mapAll.get(UPDATE_TIMESTAMP));
//                map.put(OPERATION, mapAll.get(OPERATION));
//                map.put(COLUMN_NAMES, mapAll.get(FIELDS));
//                List<Object> list = (List<Object>) mapAll.get(AFTER_IMAGES);
//                List<Object> newList = list.stream().map(obj -> {
//                    if (obj == null) {
//                        return new HashMap<String, String>() {{
//                            put("value", null);
//                        }};
//                    } else {
//                        return obj;
//                    }
//                }).collect(Collectors.toList());
//                map.put(COLUMN_VALUES, newList);

        messageStream.filter(str -> !str.isEmpty()).map(new MapFunction<String, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(String jsonStr) throws Exception {
                Map<String, Object> drsJsonMap = JSON_MAPPER.readValue(jsonStr, HashMap.class);
                String version = String.valueOf(drsJsonMap.get(DRS_VERSION));
                String seqno = String.valueOf(drsJsonMap.get(DRS_SEQ_NO));
                String shardId = String.valueOf(drsJsonMap.get(DRS_SHARD_ID));
                String eventId = String.valueOf(drsJsonMap.get(DRS_EVENT_ID));
                String updateTimestamp = String.valueOf(drsJsonMap.get(DRS_UPDATE_TIMESTAMP));
                String tableName = StringUtils.remove(String.valueOf(drsJsonMap.get(DRS_TABLE_NAME)), '\"');
                String operation = String.valueOf(drsJsonMap.get(DRS_OPERATION));
                String columnCount = String.valueOf(drsJsonMap.get(DRS_COLUMN_COUNT));
                List<Object> fields = drsJsonMap.get(DRS_FIELDS) != null ? (List<Object>) drsJsonMap.get(DRS_FIELDS) : null;
                List<Object> beforeImages = drsJsonMap.get(DRS_BEFORE_IMAGES) != null ? (List<Object>) drsJsonMap.get(DRS_BEFORE_IMAGES) : null;
                List<Object> afterImages = drsJsonMap.get(DRS_AFTER_IMAGES) != null ? (List<Object>) drsJsonMap.get(DRS_AFTER_IMAGES) : null;

                if (fields == null) {
                    return null;
                }

                String op = null;
                if (operation.equalsIgnoreCase("INSERT")) {
                    op = "c";
                }
                if (operation.equalsIgnoreCase("UPDATE")) {
                    op = "u";
                }
                if (operation.equalsIgnoreCase("DELETE")) {
                    op = "d";
                }
                if (op == null) {
                    return null;
                }

                Map<String, Object> debeziumJsonMap = new HashMap<>();
                debeziumJsonMap.put(DEBEZIUM_OP, op);
                debeziumJsonMap.put(DEBEZIUM_TS_MS, Long.parseLong(updateTimestamp));

                Map<String, Object> debeziumSourceJsonMap = new HashMap<>();
                debeziumSourceJsonMap.put(DRS_VERSION, version);
                debeziumSourceJsonMap.put(DRS_SEQ_NO, seqno);
                debeziumSourceJsonMap.put(DRS_SHARD_ID, shardId);
                debeziumSourceJsonMap.put(DRS_EVENT_ID, eventId);
                debeziumSourceJsonMap.put(DRS_UPDATE_TIMESTAMP, updateTimestamp);
                debeziumSourceJsonMap.put(DRS_OPERATION, op);
                debeziumSourceJsonMap.put(DRS_TABLE_NAME, tableName);
                debeziumSourceJsonMap.put(DRS_COLUMN_COUNT, columnCount);
                debeziumJsonMap.put(DEBEZIUM_SOURCE, debeziumSourceJsonMap);

                List<String> fieldStrList = fields.stream()
                        .map(field -> ((Map<String, String>) field).get("name")).collect(Collectors.toList());

                if (beforeImages == null) {
                    debeziumJsonMap.put(DEBEZIUM_BEFORE, null);
                } else {
                    List<String> beforeStrList = beforeImages.stream()
                            .map(before -> {
                                if (before == null) {
                                    return null;
                                }
                                return ((Map<String, String>) before).get("value");
                            }).collect(Collectors.toList());
                    Map<String, String> debeziumBeforeJsonMap = new HashMap<>();
                    for (int i = 0; i < Integer.parseInt(columnCount); i++) {
                        debeziumBeforeJsonMap.put(fieldStrList.get(i), beforeStrList.get(i));
                    }
                    debeziumJsonMap.put(DEBEZIUM_BEFORE, debeziumBeforeJsonMap);
                }

                if (afterImages == null) {
                    debeziumJsonMap.put(DEBEZIUM_AFTER, null);
                } else {
                    List<String> afterStrList = afterImages.stream()
                            .map(after -> {
                                if (after == null) {
                                    return null;
                                }
                                return ((Map<String, String>) after).get("value");
                            }).collect(Collectors.toList());
                    Map<String, String> debeziumAfterJsonMap = new HashMap<>();
                    for (int i = 0; i < Integer.parseInt(columnCount); i++) {
                        debeziumAfterJsonMap.put(fieldStrList.get(i), afterStrList.get(i));
                    }
                    debeziumJsonMap.put(DEBEZIUM_AFTER, debeziumAfterJsonMap);
                }

                return debeziumJsonMap;
            }
        }).filter(Objects::nonNull).setParallelism(parallelism)
            .addSink(new FlinkKafkaProducer<>(jsonTopic, (KafkaSerializationSchema<Map<String, Object>>) (jsonMap, aLong) -> {
                byte[] key = null;
                byte[] value = null;
                try {
                    key = ((Map<String, String>) jsonMap.get(DEBEZIUM_SOURCE)).get(DRS_TABLE_NAME).getBytes();
                    value = JSON_MAPPER.writeValueAsBytes(jsonMap);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                return new ProducerRecord<>(jsonTopic, key, value);
        }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        env.execute();

//        DataStreamSource<Map<String, Object>> messageStream =
//                env.addSource(
//                        new FlinkKafkaConsumer<>(
//                                avroTopic, new CustomizedDeSerializationSchema(), props));
//        messageStream.filter(map -> !map.isEmpty())
//                .addSink(new FlinkKafkaProducer<>(jsonTopic, new CustomizedSerializationSchema(), props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
//        env.execute();


    }
}
