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

import java.security.SecureRandom;
import java.util.Map;

/**
 * 生成随机字符串的spout
 *
 * @since 2020-09-30
 */
public class RandomSentenceSpout extends BaseRichSpout {
    private static final long serialVersionUID = 311890961371618049L;

    private SpoutOutputCollector collector;

    private SecureRandom random;

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new SecureRandom();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String[] sentences =
                new String[] {
                    "the cow jumped over the moon",
                    "an apple a day keeps the doctor away",
                    "four score and seven years ago",
                    "snow white and the seven dwarfs",
                    "i am at two with nature"
                };
        String sentence = sentences[random.nextInt(sentences.length)];
        collector.emit(new Values(sentence));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(Object id) {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void fail(Object id) {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
