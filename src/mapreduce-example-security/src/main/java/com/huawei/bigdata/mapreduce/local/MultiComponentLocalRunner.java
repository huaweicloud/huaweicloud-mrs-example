package com.huawei.bigdata.mapreduce.local;

import com.huawei.bigdata.examples.util.JarFinderUtil;
import com.huawei.bigdata.examples.util.KerberosUtil;
import com.huawei.bigdata.mapreduce.examples.MultiComponentExample;
import com.huawei.bigdata.mapreduce.examples.MultiComponentExample.MultiComponentMapper;
import com.huawei.bigdata.mapreduce.examples.MultiComponentExample.MultiComponentReducer;
import com.huawei.bigdata.mapreduce.tools.LoginUtil;
import com.huawei.bigdata.mapreduce.tools.TarManager;
import com.huawei.bigdata.mapreduce.tools.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.auth.HiveAuthConstants;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.util.Properties;

public class MultiComponentLocalRunner {

    public static String ZOOKEEPER_SERVER_PRINCIPAL_DEFAULT = null;
    private static void cleanupBeforeRun(Configuration config) throws IOException {
        FileSystem tempFS = FileSystem.get(config);
        tempFS.delete(new Path(MultiComponentExample.baseDir, MultiComponentExample.OUTPUT_DIR_NAME), true);
    }

    public static void main(String[] args) throws Exception {
        // Create JAR
        TarManager.createJar();
        Configuration config = new Configuration();

        // load parameter
        Properties clientInfo = null;
        try {
            clientInfo = new Properties();
            clientInfo.load(MultiComponentExample.class.getClassLoader().getResourceAsStream("hiveclient.properties"));
        } catch (Exception e) {
            throw new IOException(e);
        }

        String zkQuorum = clientInfo.getProperty("zk.quorum");
        String zooKeeperNamespace = clientInfo.getProperty("zooKeeperNamespace");
        String serviceDiscoveryMode = clientInfo.getProperty("serviceDiscoveryMode");
        String principal = clientInfo.getProperty("principal");
        String auth = clientInfo.getProperty("auth");
        String sasl_qop = clientInfo.getProperty("sasl.qop");

        //
        String hbaseKeytab = MultiComponentLocalRunner.class.getClassLoader().getResource("user.keytab").getPath();
        String hbaseJaas = MultiComponentLocalRunner.class.getClassLoader().getResource("jaas_mr.conf").getPath();

        String hiveClientProperties = MultiComponentLocalRunner.class.getClassLoader()
                .getResource("hiveclient.properties").getPath();
        String hbaseSite = MultiComponentLocalRunner.class.getClassLoader().getResource("hbase-site.xml").getPath();
        String hiveSite = MultiComponentLocalRunner.class.getClassLoader().getResource("hive-site.xml").getPath();

        // a list of files, separated by comma
        String files = "file://" + MultiComponentExample.KEYTAB + "," + "file://" + MultiComponentExample.KRB + ","
                + "file://" + MultiComponentExample.JAAS;
        files = files + "," + "file://" + hbaseKeytab;
        files = files + "," + "file://" + hbaseJaas;
        files = files + "," + "file://" + hiveClientProperties;
        files = files + "," + "file://" + hiveSite;
        files = files + "," + "file://" + hbaseSite;

        // this setting will ask Job upload these files to HDFS

        config.set("tmpfiles", files);

        // Security login
        LoginUtil.login(MultiComponentExample.PRINCIPAL, MultiComponentExample.KEYTAB, MultiComponentExample.KRB,
                config);
        LoginUtil.setJaasConf(MultiComponentExample.ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME,
                MultiComponentExample.PRINCIPAL, hbaseKeytab);
        ZOOKEEPER_SERVER_PRINCIPAL_DEFAULT = "zookeeper/hadoop." + KerberosUtil.getKrb5DomainRealm().toLowerCase();
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_DEFAULT);

        // clean control files before job submit
        cleanupBeforeRun(config);

        // find dependency jars for hive
        Class hiveDriverClass = Class.forName("org.apache.hive.jdbc.HiveDriver");
        Class thriftClass = Class.forName("org.apache.thrift.TException");
        Class serviceThriftCLIClass = Class.forName("org.apache.hive.service.rpc.thrift.TCLIService");
        Class hiveConfClass = Class.forName("org.apache.hadoop.hive.conf.HiveConf");
        Class hiveTransClass = Class.forName("org.apache.thrift.transport.HiveTSaslServerTransport");
        Class hiveMetaClass = Class.forName("org.apache.hadoop.hive.metastore.api.MetaException");
        Class hiveShimClass = Class.forName("org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge23");
        Class thriftCLIClass = Class.forName("org.apache.hive.service.cli.thrift.ThriftCLIService");
        Class thriftType = Class.forName("org.apache.hadoop.hive.serde2.thrift.Type");
        Class stringClass = Class.forName("org.apache.commons.lang.StringUtils");
        // add dependency jars to Job
        JarFinderUtil.addDependencyJars(config, hiveDriverClass, serviceThriftCLIClass, thriftCLIClass, thriftClass,
                hiveConfClass, hiveTransClass, hiveMetaClass, hiveShimClass, thriftType, stringClass);

        // add hive config file
        // config.addResource("hive-site.xml");
        config.addResource(MultiComponentExample.class.getClassLoader().getResourceAsStream("hive-site.xml"));
        // add hbase config file
        Configuration conf = HBaseConfiguration.create(config);
        Utils.handleZkSslEnabled(conf);

        // Initialize the job object.
        Job job = Job.getInstance(conf);
        String dir = System.getProperty("user.dir");
        job.setJar(dir + File.separator + "mapreduce-examples.jar");
        job.setJarByClass(MultiComponentExample.class);

        // set mapper&reducer class
        job.setMapperClass(MultiComponentMapper.class);
        job.setReducerClass(MultiComponentReducer.class);

        // set job input&output
        FileInputFormat.addInputPath(job, new Path(MultiComponentExample.baseDir,
                MultiComponentExample.INPUT_DIR_NAME + File.separator + "data.txt"));
        FileOutputFormat.setOutputPath(job,
                new Path(MultiComponentExample.baseDir, MultiComponentExample.OUTPUT_DIR_NAME));

        // Set the output type of the job.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // HBase use this utility class to add dependecy jars of hbase to MR job
        TableMapReduceUtil.addDependencyJars(job);

        // this is mandatory when access to security HBase cluster
        // HBase add security credentials to Job, and will use in map and reduce
        TableMapReduceUtil.initCredentials(job);

        // create Hive security credentials
        // final String zkQuorum =
        // "192.168.255.42:24002,192.168.230.195:24002,192.168.251.26:24002";
        StringBuilder sBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");
        sBuilder.append(";serviceDiscoveryMode=").append(serviceDiscoveryMode).append(";zooKeeperNamespace=")
                .append(zooKeeperNamespace).append(";sasl.qop=").append(sasl_qop).append(";auth=").append(auth)
                .append(";principal=").append(principal).append(";");
        String url = sBuilder.toString();
        Connection connection = DriverManager.getConnection(url, "", "");
        String tokenStr = ((HiveConnection) connection).getDelegationToken(
                UserGroupInformation.getCurrentUser().getShortUserName(), MultiComponentExample.PRINCIPAL);
        connection.close();
        Token<DelegationTokenIdentifier> hive2Token = new Token<DelegationTokenIdentifier>();
        hive2Token.decodeFromUrlString(tokenStr);
        // add Hive security credentials to Job
        job.getCredentials().addToken(new Text("hive.server2.delegation.token"), hive2Token);
        job.getCredentials().addToken(new Text(HiveAuthConstants.HS2_CLIENT_TOKEN), hive2Token);

        // Submit the job to a remote environment for execution.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
