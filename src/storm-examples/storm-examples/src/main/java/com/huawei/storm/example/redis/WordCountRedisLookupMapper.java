/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.redis;

import com.google.common.collect.Lists;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.List;

class WordCountRedisLookupMapper implements RedisLookupMapper {
    /**
     *
     */
    private static final long serialVersionUID = 2456517351356120320L;

    private RedisDataTypeDescription description;

    public WordCountRedisLookupMapper() {
        // 指定数据类型
        description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING);
    }

    @Override
    public List<Values> toTuple(ITuple input, Object value) {
        // 逻辑运算，在redis中查找单词出现次数并加1，返回出现次数。
        if (value == null) {
            value = 0;

        } else {
            try {
                value = Integer.parseInt(value.toString()) + 1;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        String member = getKeyFromTuple(input);
        List<Values> values = Lists.newArrayList();
        values.add(new Values(member, value.toString()));
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        // 获得数据类型
        return description;
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        // 获得key值
        return tuple.getStringByField("word");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        // 获得value值
        return null;
    }
}
