package com.huawei.bigdata.flink.examples.cep;

import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.table.data.RowData;

public class PatternProcessorRowData implements PatternProcessor<RowData> {
    private final String id;
    private final Pattern<RowData, RowData> pattern;
    private final PatternProcessFunction<RowData, RowData> patternProcessFunction;
    private final int version;

    public PatternProcessorRowData(
            String id,
            Pattern<RowData, RowData> pattern,
            PatternProcessFunction<RowData, RowData> patternProcessFunction,
            int version) {
        this.id = id;
        this.pattern = pattern;
        this.patternProcessFunction = patternProcessFunction;
        this.version = version;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Pattern<RowData, ?> getPattern(ClassLoader classLoader) {
        return pattern;
    }

    @Override
    public PatternProcessFunction<RowData, ?> getPatternProcessFunction() {
        return patternProcessFunction;
    }

    @Override
    public int getVersion() {
        return version;
    }
}
