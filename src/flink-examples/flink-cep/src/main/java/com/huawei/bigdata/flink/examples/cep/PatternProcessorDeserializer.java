package com.huawei.bigdata.flink.examples.cep;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

/* * Simple deserializer for {@link PatternProcessorRowData}. */
public class PatternProcessorDeserializer implements Deserializer<PatternProcessorRowData> {
    @Override
    public PatternProcessorRowData deserialize(String topic, byte[] bytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream in = new ObjectInputStream(bais)) {
            return (PatternProcessorRowData) in.readObject();
        } catch (Exception e) {
            throw new SerializationException("Could not deserialize the serialized PatternProcessor");
        }
    }
}
