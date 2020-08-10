package com.huawei.bigdata.kafka.example;

import kafka.producer.Partitioner;
import kafka.utils.*;

public class SimplePartitioner implements Partitioner
{
    public SimplePartitioner(VerifiableProperties props)
    {
    }
    
    public int partition(Object key, int numPartitions)
    {
        int partition = 0;
        String partitionKey = (String) key;
        
        try
        {
            //指定分区逻辑，也就是key
            partition = Integer.parseInt(partitionKey) % numPartitions;
        }
        catch (NumberFormatException ne)
        {
            //如果解析失败，都分配到0分区上
            partition = 0;
        }
        
        return partition;
    }
}
