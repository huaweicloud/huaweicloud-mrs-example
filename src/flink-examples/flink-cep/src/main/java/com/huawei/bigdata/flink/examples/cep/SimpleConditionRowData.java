package com.huawei.bigdata.flink.examples.cep;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.table.data.RowData;

public class SimpleConditionRowData extends SimpleCondition<RowData> {
    private final int valueToFilter;

    public SimpleConditionRowData(int valueToFilter) {
        this.valueToFilter = valueToFilter;
    }

    @Override
    public boolean filter(RowData value) throws Exception {
        return value.getInt(1) == valueToFilter;
    }

    @Override
    public String toString() {
        return "Pattern condition with valueToFilter=" + this.valueToFilter;
    }
}
