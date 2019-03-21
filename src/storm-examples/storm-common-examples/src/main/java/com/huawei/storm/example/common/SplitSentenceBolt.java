package com.huawei.storm.example.common;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 单词拆分算子
 */
public class SplitSentenceBolt extends BaseBasicBolt
{
    
    private static final long serialVersionUID = 2381767905191703644L;

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for (String word : words)
        {
            word = word.trim();
            if (!word.isEmpty())
            {
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word"));
    }
}
