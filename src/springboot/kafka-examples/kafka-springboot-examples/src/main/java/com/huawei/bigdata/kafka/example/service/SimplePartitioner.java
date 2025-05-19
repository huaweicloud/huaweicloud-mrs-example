/**
 * Copyright Notice:
 *      Copyright  2013-2024, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */

package com.huawei.bigdata.kafka.example.service;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区
 **/
public class SimplePartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partition = 0;
        String partitionKey = (String) key;
        int numPartitions = cluster.partitionsForTopic(topic).size();

        try {
            // 指定分区逻辑，即key
            partition = Integer.parseInt(partitionKey) % numPartitions;
        } catch (NumberFormatException ne) {
            // 如果解析失败，partition = 0 都分配到0号分区上
        }

        return partition;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
