package com.huawei.bigdata.flink.examples.cep;

import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

public class PatternProcessFunctionRowData extends PatternProcessFunction<RowData, RowData> {
    @Override
    public void processMatch(Map<String, List<RowData>> match, Context ctx, Collector<RowData> out) {
        GenericRowData outRow = new GenericRowData(2);

        StringBuilder ids = new StringBuilder();
        int id = Integer.MIN_VALUE;
        for (Map.Entry<String, List<RowData>> entry : match.entrySet()) {
            for (RowData row : entry.getValue()) {
                ids.append(row.getInt(0));
                if (id != Integer.MIN_VALUE) {
                    Preconditions.checkState(id == row.getInt(1));
                }
                id = row.getInt(1);
            }
        }

        outRow.setField(0, StringData.fromString(ids.toString()));
        outRow.setField(1, id);
        out.collect(outRow);
    }
}
