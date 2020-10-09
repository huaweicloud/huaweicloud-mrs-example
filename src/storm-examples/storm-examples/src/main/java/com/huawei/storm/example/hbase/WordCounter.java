/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.hbase;

import static org.apache.storm.utils.Utils.tuple;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
*  @author
*  @since
*/
public class WordCounter implements IBasicBolt {
    /**
     * @param stormConf storm配置
     * @param context  上下文
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {}

    /**
     * @param input input
     * @param collector collector
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        collector.emit(tuple(input.getValues().get(0), 1));
    }

    /**
     *
     */
    public void cleanup() {}

    /**
     * @param declarer declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
