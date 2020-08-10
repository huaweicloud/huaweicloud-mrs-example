package com.huawei.storm.example.kafka;

import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class CountBolt extends BaseBasicBolt{

private static final long serialVersionUID = 668785327917400661L;
    
    private Map<String, Integer> counts = Maps.newHashMap();

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector)
    {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null)
        {
            count = 0;
        }
        count++;
        counts.put(word, count);
        collector.emit(new Values(word,word + "," + count.toString()));
        System.out.println("word: " + word + ", count: " + count);
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "count"));
    }
    
}
