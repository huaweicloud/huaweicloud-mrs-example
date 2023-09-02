package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerRecordDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<byte[], byte[]>> {
    @Override
    public boolean isEndOfStream(ConsumerRecord<byte[], byte[]> consumerRecord) {
        return false;
    }

    @Override
    public ConsumerRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return consumerRecord;
    }

    @Override
    public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
        return TupleTypeInfo.of(new TypeHint<ConsumerRecord<byte[], byte[]>>() {});
    }
}
