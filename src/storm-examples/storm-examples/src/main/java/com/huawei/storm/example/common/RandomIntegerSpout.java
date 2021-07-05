/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.common;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Map;

/**
 * Emits a random integer and a timestamp value (offset by one day),
 * every 100 ms. The ts field can be used in tuple time based windowing.
 *
 * @since 2020-09-30
 */

public class RandomIntegerSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomIntegerSpout.class);
    private SpoutOutputCollector collector;
    private SecureRandom rand;
    private long msgId = 0;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value", "ts", "msgid"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new SecureRandom();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        collector.emit(
                new Values(rand.nextInt(1000), System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
    }

    @Override
    public void ack(Object msgId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Got ACK for msgId : " + msgId);
        }
    }

    @Override
    public void fail(Object msgId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Got FAIL for msgId : " + msgId);
        }
    }
}
