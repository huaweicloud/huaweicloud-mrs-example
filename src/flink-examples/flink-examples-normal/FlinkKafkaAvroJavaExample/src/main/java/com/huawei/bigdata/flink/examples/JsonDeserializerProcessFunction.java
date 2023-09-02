package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class JsonDeserializerProcessFunction<OUT> extends ProcessFunction<ConsumerRecord<byte[], byte[]>, OUT>
    implements ResultTypeQueryable<OUT> {

    private ObjectReader reader;
    private final Class<OUT> clazz;
    private final TypeInformation<OUT> information;

    public JsonDeserializerProcessFunction(Class<OUT> clazz) {
        this(clazz, TupleTypeInfo.of(clazz));
    }

    public JsonDeserializerProcessFunction(Class<OUT> clazz, TypeInformation<OUT> information) {
        this.clazz = clazz;
        this.information = information;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        this.reader = mapper.readerFor(clazz);
    }

    @Override
    public void processElement(ConsumerRecord<byte[], byte[]> consumerRecord, Context context, Collector<OUT> collector)
        throws Exception {
        byte[] key = consumerRecord.key();
        byte[] value = consumerRecord.value();
        OUT raw = reader.readValue(value);
        collector.collect(raw);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return information;
    }
}
