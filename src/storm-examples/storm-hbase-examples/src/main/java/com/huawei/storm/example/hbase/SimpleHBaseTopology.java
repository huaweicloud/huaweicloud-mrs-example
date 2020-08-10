package com.huawei.storm.example.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.storm.example.common.AuthenticationType;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class SimpleHBaseTopology {

    private static final String DEFAULT_TABLE_NAME = "WordCount";
    private static final String DEFAULT_ROWKEY_FIELD = "word";
    private static final String DEFAULT_COLUMN_FIELD = "word";
    private static final String DEFAULT_COUNTER_FIELD = "count";
    private static final String DEFAULT_COLUMN_FAMILY = "cf";
    
    private static final String HBASE_CONF = "hbase.conf";
    private static final String HBASE_ROOT_DIR = "hbase.rootdir";
    
    private static final String SECURITY_AUTO_KEYTAB_PLUGIN = "org.apache.storm.security.auth.kerberos.AutoTGTFromKeytab";
    private static final String SECURITY_AUTO_HBASE_PLUGIN = "org.apache.storm.hbase.security.AutoHBase";
    private static final String SECURITY_AUTO_TGT_PLUGIN = "org.apache.storm.security.auth.kerberos.AutoTGT";

    public static void main(String[] args) throws Exception 
    {
        Config conf = new Config();
        
        //增加kerberos认证所需的plugin到列表中，安全模式必选
        //当前支持keytab方式和ticketCache方式两种认证方式
        setSecurityConf(conf,AuthenticationType.KEYTAB);
        
        if(args.length >= 2)
        {
            //用户更改了默认的keytab文件名，这里需要将新的keytab文件名通过参数传入
            conf.put(Config.TOPOLOGY_KEYTAB_FILE, args[1]);
        }
        //hbase的客户端配置，这里只提供了“hbase.rootdir”配置项，参数可选
        Map<String, Object> hbConf = new HashMap<String, Object>();
        if(args.length >= 3)
        {
            hbConf.put(HBASE_ROOT_DIR, args[2]);
        }
        //必配参数，若用户不输入，则该项为空
        conf.put(HBASE_CONF, hbConf);
        
        //spout为随机单词spout
        WordSpout spout = new WordSpout();
        WordCounter bolt = new WordCounter();

        //HbaseMapper，用于解析tuple内容
        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField(DEFAULT_ROWKEY_FIELD)
                .withColumnFields(new Fields(DEFAULT_COLUMN_FIELD))
                .withCounterFields(new Fields(DEFAULT_COUNTER_FIELD))
                .withColumnFamily(DEFAULT_COLUMN_FAMILY);

        //HBaseBolt，第一个参数为表名
        //withConfigKey("hbase.conf")将hbase的客户端配置传入HBaseBolt
        HBaseBolt hbase = new HBaseBolt(DEFAULT_TABLE_NAME, mapper).withConfigKey(HBASE_CONF);


        // wordSpout ==> countBolt ==> HBaseBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word-spout", spout, 1);
        builder.setBolt("count-bolt", bolt, 1).shuffleGrouping("word-spout");
        builder.setBolt("hbase-bolt", hbase, 1).fieldsGrouping("count-bolt", new Fields("word"));
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
                auto_tgts.add(SECURITY_AUTO_HBASE_PLUGIN);
            }
        }
        conf.put(Config.TOPOLOGY_AUTO_CREDENTIALS, auto_tgts);
    }
    
}
