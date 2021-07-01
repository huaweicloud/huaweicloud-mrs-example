/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.kafka;

import com.google.common.collect.Maps;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author
 * @since
 */
public class CountBolt extends BaseBasicBolt {
    private static final Logger logger = Logger.getLogger(CountBolt.class);
    private static final long serialVersionUID = 668785327917400661L;

    private Map<String, Integer> counts = Maps.newHashMap();

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        counts.put(word, count);
        collector.emit(new Values(word, word + "," + count.toString()));
        logger.info(String.valueOf("word: " + word + ", count: " + count));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
