package com.huawei.storm.example.wordcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;


/**
 * 单词计数器算子
 */
public class WordCountBolt extends BaseBasicBolt
{
    private static final long serialVersionUID = 668785327917400661L;
    
    private Map<String, Integer> counts = new HashMap<String, Integer>();
    
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
