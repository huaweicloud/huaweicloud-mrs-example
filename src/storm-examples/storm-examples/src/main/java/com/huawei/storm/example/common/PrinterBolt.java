/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.common;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 *
 * @since 2020-09-30
 */
public class PrinterBolt extends BaseBasicBolt {
    private static final Logger logger = Logger.getLogger(PrinterBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        logger.info(String.valueOf(tuple));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {}
}
