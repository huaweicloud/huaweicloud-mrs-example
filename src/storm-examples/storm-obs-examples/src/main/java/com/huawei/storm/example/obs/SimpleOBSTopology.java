package com.huawei.storm.example.obs;

import com.huawei.storm.example.common.RandomSentenceSpout;
import com.huawei.storm.example.common.SplitSentenceBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class SimpleOBSTopology {
    
    private static final String HDFS_TEMP_PATH = "/usr/foo/";

    public static void main(String[] args) throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
    
        // 分隔符格式，当前采用“|”代替默认的“，”对tuple中的filed进行分隔
        // HdfsBolt必选参数
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        // 同步策略，每1000个tuple对文件系统进行一次同步
        // HdfsBolt必选参数
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // 文件大小循环策略，当文件大小到达5KB时，从头开始写
        // HdfsBolt必选参数
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.KB);

        // 写入obs的目的文件
        // HdfsBolt必选参数
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(HDFS_TEMP_PATH);


        //创建HdfsBolt, FsUrl设置格式s3a://bucket-name，其中bucket-name设置为实际桶值
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(args[0])
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        //spout为随机语句spout
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("count", bolt, 1).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
    
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 1);

        //命令行提交拓扑
        StormSubmitter.submitTopology("mytest", conf, builder.createTopology());
    }

}
