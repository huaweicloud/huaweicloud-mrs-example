package com.huawei.bigdata.kafka.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class SimplePartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partition = 0;
        String partitionKey = (String) key;
        int numPartitions = cluster.partitionsForTopic(topic).size();

        try {
            //指定分区逻辑，也就是key
            partition = Integer.parseInt(partitionKey) % numPartitions;
        } catch (NumberFormatException ne) {
            //如果解析失败，都分配到0分区上
            partition = 0;
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
