/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

class WordCountStoreMapper implements RedisStoreMapper {
    /**
     *
     */
    private static final long serialVersionUID = -5058859861148439814L;

    private RedisDataTypeDescription description;

    public WordCountStoreMapper() {
        // 指定数据输入类型
        description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        // 返回数据类型
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
        return tuple.getStringByField("count");
    }
}
