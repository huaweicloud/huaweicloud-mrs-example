/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.bigdata.ctbase.examples;

import org.apache.spark.serializer.KryoRegistrator;

/**
 * Define serializer class.
 */
public class MyRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(com.esotericsoftware.kryo.Kryo kryo) {
        kryo.register(org.apache.hadoop.conf.Configuration.class);
        kryo.register(scala.collection.mutable.ListBuffer.class);
        kryo.register(org.apache.hadoop.hbase.client.Result.class);
        kryo.register(org.apache.hadoop.hbase.Cell.class);
    }
}
