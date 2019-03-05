package com.huawei.storm.example.jdbc;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class JDBCSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    public static final String[] words = new String[] { "apple", "orange", "pineapple", "banana", "watermelon" };

    public JDBCSpout() {
        this(true);
    }

    public JDBCSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        final Random rand = new Random();
        final String word = words[rand.nextInt(words.length)];
        this.collector.emit(new Values(word), UUID.randomUUID());
        Thread.yield();
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("WORD"));
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
