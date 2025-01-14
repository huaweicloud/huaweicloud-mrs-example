package com.huawei.bigdata.flink.examples.cep;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

/* * Simple serializer for {@link PatternProcessorRowData}. */
public class PatternProcessorSerializer implements Serializer<PatternProcessorRowData> {
    @Override
    public byte[] serialize(String topic, PatternProcessorRowData rowDataPatternProcessor) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(rowDataPatternProcessor);
            out.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Cannot serialize PatternProcessor " + rowDataPatternProcessor);
        }
    }
}
