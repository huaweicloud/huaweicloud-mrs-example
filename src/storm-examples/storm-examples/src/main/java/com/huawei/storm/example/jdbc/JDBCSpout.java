/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.jdbc;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.security.SecureRandom;
import java.util.Map;
import java.util.UUID;

/**
*
*/
public class JDBCSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;

    /**
     *
     */
    public static final String[] WORDS = new String[] {"apple", "orange", "pineapple", "banana", "watermelon"};

    public JDBCSpout() {
        this(true);
    }

    public JDBCSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }

    /**
     * @param conf 配置
     * @param context 上下文
     * @param collector collector
     */
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     *
     */
    public void close() {}

    /**
     *
     */
    public void nextTuple() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        final SecureRandom rand = new SecureRandom();
        final String word = WORDS[rand.nextInt(WORDS.length)];
        this.collector.emit(new Values(word), UUID.randomUUID());
        Thread.yield();
    }

    /**
     * @param msgId  msgId
     */
    public void ack(Object msgId) {}

    /**
     * @param msgId msgId
     */
    public void fail(Object msgId) {}

    /**
     * @param declarer out put fields declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("WORD"));
    }

    /**
     *
     */
    @Override
    public void activate() {}

    /**
     *
     */
    @Override
    public void deactivate() {}

    /**
     *
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
