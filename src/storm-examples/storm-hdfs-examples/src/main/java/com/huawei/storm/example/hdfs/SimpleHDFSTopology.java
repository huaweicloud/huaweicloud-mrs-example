package com.huawei.storm.example.hdfs;

import java.util.ArrayList;
import java.util.List;

import com.huawei.storm.example.common.AuthenticationType;
import com.huawei.storm.example.common.RandomSentenceSpout;
import com.huawei.storm.example.common.SplitSentenceBolt;

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

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class SimpleHDFSTopology {
    
    /*
     *HDFS文件系统的默认url，联邦场景下可以通过修改该值来访问相应的NameService
     */
    private static final String DEFAULT_FS_URL = "hdfs://hacluster";
    private static final String HDFS_TEMP_PATH = "/user/foo/";
    
    private static final String SECURITY_AUTO_KEYTAB_PLUGIN = "org.apache.storm.security.auth.kerberos.AutoTGTFromKeytab";
    private static final String SECURITY_AUTO_HDFS_PLUGIN = "org.apache.storm.hdfs.common.security.AutoHDFS";
    private static final String SECURITY_AUTO_TGT_PLUGIN = "org.apache.storm.security.auth.kerberos.AutoTGT";
    
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

        // 文件大小循环策略，当文件大小到达5M时，从头开始写
        // HdfsBolt必选参数
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

        // 写入hdfs的目的文件
        // HdfsBolt必选参数
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(HDFS_TEMP_PATH);


        //创建HdfsBolt
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(DEFAULT_FS_URL)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
    
        //spout为随机语句spout
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("count", bolt, 1).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
    
        //增加kerberos认证所需的plugin到列表中，安全模式必选
        //当前支持keytab方式和ticketCache方式两种认证方式
        setSecurityConf(conf, AuthenticationType.KEYTAB);
    
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 1);
    
        if(args.length >= 2)
        {
            //用户更改了默认的keytab文件名，这里需要将新的keytab文件名通过参数传入
            conf.put(Config.TOPOLOGY_KEYTAB_FILE, args[1]);
        }

        //命令行提交拓扑
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    
    private static void setSecurityConf(Config conf, AuthenticationType type) {
        List<String> auto_tgts = new ArrayList<String>();

        switch (type) 
        {
            case KEYTAB: 
            {
                auto_tgts.add(SECURITY_AUTO_KEYTAB_PLUGIN);
                break;
            }
            case TICKET_CACHE: {
                auto_tgts.add(SECURITY_AUTO_TGT_PLUGIN);
                auto_tgts.add(SECURITY_AUTO_HDFS_PLUGIN);
            }
        }
        conf.put(Config.TOPOLOGY_AUTO_CREDENTIALS, auto_tgts);
    }
    
}
