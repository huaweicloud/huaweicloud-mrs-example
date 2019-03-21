package com.huawei.storm.example.wordcount;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import com.huawei.storm.example.common.RandomSentenceSpout;
import com.huawei.storm.example.common.SplitSentenceBolt;
import com.huawei.storm.example.common.SubmitType;
import com.huawei.storm.example.security.LoginUtil;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;


/**
 * Storm原生API示例
 */
public class WordCountTopology
{
    /**
     * topology name
     */
    private static final String TOPOLOGY_NAME = "word-count";
    
    /**
     * The jass.conf for zookeeper client security login.
     */
    public static final String ZOOKEEPER_AUTH_JASSCONF = "java.security.auth.login.config";
    
    /**
     * Zookeeper quorum principal.
     */
    public static final String ZOOKEEPER_AUTH_PRINCIPAL = "zookeeper.server.principal";
    
    /**
     * java security krb5 file path
     */
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    
    /**
     * 提交storm应用程序的时候，jar包所在的property名称
     */
    private static final String STORM_SUBMIT_JAR_PROPERTY = "storm.jar";
    
    public static void main(String[] args)
        throws Exception
    {
        TopologyBuilder builder = buildTopology();
        
        /*
         * 任务的提交认为三种方式
         * 1、命令行方式提交，这种需要将应用程序jar包复制到客户端机器上执行客户端命令提交
         * 2、远程方式提交，这种需要将应用程序的jar包打包好之后在eclipse中运行main方法提交
         * 3、本地提交 ，在本地执行应用程序，一般用来测试
         * 命令行方式和远程方式安全和非安全模式都支持
         * 本地提交仅支持非安全模式
         * 
         * 用户同时只能选择一种任务提交方式，默认命令行方式提交，如果是其他方式，请删除代码注释即可
         */
        
        submitTopology(builder, SubmitType.CMD);
    }
    
    private static void submitTopology(TopologyBuilder builder, SubmitType type) throws Exception
    {
        switch (type) 
        {
            case CMD: 
            {
                cmdSubmit(builder, null);
                break;
            }
            case REMOTE:
            {
                remoteSubmit(builder);
                break;
            }
            case LOCAL:
            {
                localSubmit(builder);
                break;
            }
        }
    }
    
    /**
     * 命令行方式远程提交
     * 步骤如下：
     * 打包成Jar包，然后在客户端命令行上面进行提交
     *   远程提交的时候，要先将该应用程序和其他外部依赖(非excemple工程提供，用户自己程序依赖)的jar包打包成一个大的jar包
     *   再通过storm客户端中storm -jar的命令进行提交
     * 
     * 如果是安全环境，客户端命令行提交之前，必须先通过kinit命令进行安全登录
     * 
     * 运行命令如下：
     * ./storm jar ../example/example.jar com.huawei.storm.example.WordCountTopology
     */    
    private static void cmdSubmit(TopologyBuilder builder, Config conf)
        throws AlreadyAliveException, InvalidTopologyException, AuthorizationException
    {
        if (conf == null)
        {
            conf = new Config();
        }
        conf.setNumWorkers(1);
        
        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, builder.createTopology());
    }
    
    
    /**
     * 通过代码配置直接远程提交
     * 1、打包成Jar包，修改下面userJarFilePath变量
     * 2、如果是安全模式,复制用户的keytab文件到客户端conf目录，修改下面userKeyTablePath和userPrincipal变量
     * 注意：keytab的principal名称一般是用户名+"@HADOOP.COM"
     * keytab文件可以在manager界面上添加和下载
     * 3、修改本机时间和集群时间一致，时间差不超过5分钟，否则安全连接会失败
     * 4、执行main方法
     */
    private static void remoteSubmit(TopologyBuilder builder)
        throws AlreadyAliveException, InvalidTopologyException, AuthorizationException,
        IOException
    {
        Config config = createConf();
        
        String userJarFilePath = "替换为用户Jar包地址";
        System.setProperty(STORM_SUBMIT_JAR_PROPERTY, userJarFilePath);
        
        //安全模式下的一些准备工作
        if (isSecurityModel())
        {
            securityPrepare(config);
        }
        config.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, config, builder.createTopology());
    }
    
    /**
     * 本地模式提交，一般用来测试
     */
    private static void localSubmit(TopologyBuilder builder)
        throws InterruptedException 
    {
        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        Thread.sleep(100000);
        cluster.shutdown();
    }

    private static Config createConf()
    {
        Map<String, Object> conf = Utils.readStormConfig();
        Config config = new Config();
        for (Entry<String, Object> et : conf.entrySet())
        {
            config.put(et.getKey(), et.getValue());
        }
        return config;
    }
    
    private static void securityPrepare(Config config)
        throws IOException
    {
        String userKeyTablePath =
            System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "resources" + File.separator + "替换为用户keytab文件名称";
        String userPrincipal = "替换为用户principal名称";
        String krbFilePath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "resources" + File.separator +"krb5.conf";
        
        //windows路径下分隔符替换
        userKeyTablePath = userKeyTablePath.replace("\\", "\\\\");
        krbFilePath = krbFilePath.replace("\\", "\\\\");
        
        String principalInstance = String.valueOf(config.get(Config.STORM_SECURITY_PRINCIPAL_INSTANCE));
        LoginUtil.setKrb5Config(krbFilePath);
        LoginUtil.setZookeeperServerPrincipal("zookeeper/" + principalInstance);
        LoginUtil.setJaasFile(userPrincipal, userKeyTablePath);
    }
    
    private static TopologyBuilder buildTopology()
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 5);
        builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));
        return builder;
    }
    
    private static Boolean isSecurityModel()
    {
        String krbFilePath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator + "resources" + File.separator +"krb5.conf";
        return new File(krbFilePath).exists();
    }
}
