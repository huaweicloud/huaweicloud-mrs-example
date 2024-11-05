package com.huawei.bigdata.mapreduce.examples;

import com.huawei.bigdata.examples.util.JarFinderUtil;
import com.huawei.bigdata.examples.util.KerberosUtil;
import com.huawei.bigdata.mapreduce.tools.LoginUtil;
import com.huawei.bigdata.mapreduce.tools.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.auth.HiveAuthConstants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * This is the Mapreduce example that operate multi-components. This example,
 * when in map phase, read hdfs file and get name of male person, then read one
 * row of HBase data and then query Hive to get related data, then concatenate
 * all values. In reduce phase, just save the map output to HBase and HDFS
 * <p>
 * The purpose is mainly guide developer how to add configuration resource and
 * how to do security authentication when product business invokes more than one
 * component
 */

public class MultiComponentExample {
    private static final Log LOG = LogFactory.getLog(MultiComponentExample.class);

    // security info
    public static final String PRINCIPAL = "test@HADOOP.COM";
    public static final String KEYTAB = MultiComponentExample.class.getClassLoader().getResource("user.keytab")
            .getPath();
    public static final String KRB = MultiComponentExample.class.getClassLoader().getResource("krb5.conf").getPath();
    public static final String JAAS = MultiComponentExample.class.getClassLoader().getResource("jaas_mr.conf")
            .getPath();
    public static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    public static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    public static  String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = null;

    // input & output form this example
    public static final String INPUT_DIR_NAME = "input";
    public static final String OUTPUT_DIR_NAME = "output";
    public static final String baseDir = "/tmp/examples/multi-components/mapreduce"; // default

    private static Configuration config = new Configuration();

    /**
     * Clean up the files before a test run
     *
     * @throws IOException on error
     */
    private static void cleanupBeforeRun() throws IOException {
        FileSystem tempFS = FileSystem.get(config);
        tempFS.delete(new Path(baseDir, OUTPUT_DIR_NAME), true);
    }

    /**
     * Mapper class that execute read operations
     */
    public static class MultiComponentMapper extends Mapper<Object, Text, Text, Text> {

        Configuration conf;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            conf = context.getConfiguration();

            // for components that depend on Zookeeper, need provide the conf of
            // jaas and krb5
            // Notice, no need to login again here, will use the credentials in
            // main function
            String krb5 = "krb5.conf";
            String jaas = "jaas_mr.conf";
            // These files are uploaded at main function
            File jaasFile = new File(jaas);
            File krb5File = new File(krb5);
            System.setProperty("java.security.auth.login.config", jaasFile.getCanonicalPath());
            System.setProperty("java.security.krb5.conf", krb5File.getCanonicalPath());
            System.setProperty("zookeeper.sasl.client", "true");

            LOG.info("UGI :" + UserGroupInformation.getCurrentUser());

            String name = "";
            String line = value.toString();
            if (line.contains("male")) {
                // A character string that has been read
                name = line.substring(0, line.indexOf(","));
            }
            // 1. read from HBase
            String hbaseData = readHBase();

            // 2. read from Hive
            String hiveData = readHive(name);

            // The Map task outputs a key-value pair.
            context.write(new Text(name), new Text("hbase:" + hbaseData + ", hive:" + hiveData));
        }

        /**
         * read for HBase table, use Get to get value from HBase
         *
         * @return result from hbase
         */
        private String readHBase() {
            String tableName = "table1";
            String columnFamily = "cf";
            String hbaseKey = "1";
            String hbaseValue;

            Configuration hbaseConfig = HBaseConfiguration.create(conf);
            org.apache.hadoop.hbase.client.Connection conn = null;
            try {
                // Create a HBase connection
                conn = ConnectionFactory.createConnection(hbaseConfig);
                // get table
                Table table = conn.getTable(TableName.valueOf(tableName));
                // Instantiate a Get object.
                Get get = new Get(hbaseKey.getBytes());
                // Submit a get request.
                Result result = table.get(get);
                hbaseValue = Bytes.toString(result.getValue(columnFamily.getBytes(), "cid".getBytes()));

                return hbaseValue;

            } catch (IOException e) {
                LOG.warn("Exception occur ", e);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (Exception e1) {
                        LOG.error("Failed to close the connection ", e1);
                    }
                }
            }

            return "";
        }

        /**
         * read from Hive, use JDBC
         *
         * @return result from hive
         */
        private String readHive(String name) throws IOException {

            // load parameter
            Properties clientInfo = null;
            String userdir = System.getProperty("user.dir") + "/";
            InputStream fileInputStream = null;
            try {
                clientInfo = new Properties();
                String hiveclientProp = userdir + "hiveclient.properties";
                File propertiesFile = new File(hiveclientProp);
                fileInputStream = new FileInputStream(propertiesFile);
                clientInfo.load(fileInputStream);
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            }
            String zkQuorum = clientInfo.getProperty("zk.quorum");
            String zooKeeperNamespace = clientInfo.getProperty("zooKeeperNamespace");
            String serviceDiscoveryMode = clientInfo.getProperty("serviceDiscoveryMode");

            // Read this carefully:
            // MapReduce can only use Hive through JDBC.
            // Hive will submit another MapReduce Job to execute query.
            // So we run Hive in MapReduce is not recommended.
            final String driver = "org.apache.hive.jdbc.HiveDriver";

            String sql = "select name,sum(stayTime) as " + "stayTime from person where name = '" + name
                    + "' group by name";

            StringBuilder sBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");
            // in map or reduce, use 'auth=delegationToken'
            sBuilder.append(";serviceDiscoveryMode=").append(serviceDiscoveryMode).append(";zooKeeperNamespace=")
                    .append(zooKeeperNamespace).append(";auth=delegationToken;");

            String url = sBuilder.toString();
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            try {
                Class.forName(driver);
                connection = DriverManager.getConnection(url, "", "");
                statement = connection.prepareStatement(sql);
                resultSet = statement.executeQuery();

                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
            } catch (ClassNotFoundException e) {
                LOG.warn("Exception occur ", e);
            } catch (SQLException e) {
                LOG.warn("Exception occur ", e);
            } finally {
                if (null != resultSet) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        // handle exception
                    }
                }
                if (null != statement) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        // handle exception
                    }
                }
                if (null != connection) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        // handle exception
                    }
                }
            }

            return "";
        }

    }

    /**
     * Reducer class that execute write operations
     */
    public static class MultiComponentReducer extends Reducer<Text, Text, Text, Text> {
        Configuration conf;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();

            // for components that depend on Zookeeper, need provide the conf of
            // jaas and krb5
            // Notice, no need to login again here, will use the credentials in
            // main function
            String krb5 = "krb5.conf";
            String jaas = "jaas_mr.conf";
            // These files are uploaded at main function
            File jaasFile = new File(jaas);
            File krb5File = new File(krb5);
            System.setProperty("java.security.auth.login.config", jaasFile.getCanonicalPath());
            System.setProperty("java.security.krb5.conf", krb5File.getCanonicalPath());
            System.setProperty("zookeeper.sasl.client", "true");

            Text finalValue = new Text("");
            // just pick the last value as the data to save
            for (Text value : values) {
                finalValue = value;
            }

            // write data to HBase
            writeHBase(key.toString(), finalValue.toString());

            // save result to HDFS
            context.write(key, finalValue);
        }

        /**
         * write to HBase table, use HBase Put
         */
        private void writeHBase(String rowKey, String data) {
            String tableName = "table1";
            String columnFamily = "cf";

            try {
                LOG.info("UGI read :" + UserGroupInformation.getCurrentUser());
            } catch (IOException e1) {
                // handler exception
            }

            Configuration hbaseConfig = HBaseConfiguration.create(conf);
            org.apache.hadoop.hbase.client.Connection conn = null;
            try {
                // Create a HBase connection
                conn = ConnectionFactory.createConnection(hbaseConfig);
                // get table
                Table table = conn.getTable(TableName.valueOf(tableName));

                // create a Put to HBase
                List<Put> list = new ArrayList<Put>();
                byte[] row = Bytes.toBytes("row" + rowKey);
                Put put = new Put(row);
                byte[] family = Bytes.toBytes(columnFamily);
                byte[] qualifier = Bytes.toBytes("value");
                byte[] value = Bytes.toBytes(data);
                put.addColumn(family, qualifier, value);
                list.add(put);
                // execute Put
                table.put(list);
            } catch (IOException e) {
                LOG.warn("Exception occur ", e);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (Exception e1) {
                        LOG.error("Failed to close the connection ", e1);
                    }
                }
            }

        }
    }

    /**
     * main function
     *
     * @param args inputs, no need in this example
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // load parameter
        Properties clientInfo = null;
        try {
            clientInfo = new Properties();
            clientInfo.load(MultiComponentExample.class.getClassLoader().getResourceAsStream("hiveclient.properties"));
        } catch (Exception e) {
            throw new IOException(e);
        } finally {

        }
        String zkQuorum = clientInfo.getProperty("zk.quorum");// zookeeper鑺傜偣ip鍜岀鍙ｅ垪琛�
        String zooKeeperNamespace = clientInfo.getProperty("zooKeeperNamespace");
        String serviceDiscoveryMode = clientInfo.getProperty("serviceDiscoveryMode");
        String principal = clientInfo.getProperty("principal");
        String auth = clientInfo.getProperty("auth");
        String sasl_qop = clientInfo.getProperty("sasl.qop");

        //
        String hbaseKeytab = MultiComponentExample.class.getClassLoader().getResource("user.keytab").getPath();
        String hbaseJaas = MultiComponentExample.class.getClassLoader().getResource("jaas_mr.conf").getPath();
        String hiveClientProperties = MultiComponentExample.class.getClassLoader().getResource("hiveclient.properties")
                .getPath();

        // a list of files, separated by comma
        String files = "file://" + KEYTAB + "," + "file://" + KRB + "," + "file://" + JAAS;
        files = files + "," + "file://" + hbaseKeytab;
        files = files + "," + "file://" + hbaseJaas;
        files = files + "," + "file://" + hiveClientProperties;

        // this setting will ask Job upload these files to HDFS
        config.set("tmpfiles", files);

        // Security login
        LoginUtil.login(PRINCIPAL, KEYTAB, KRB, config);
        LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, PRINCIPAL, hbaseKeytab);

        ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL= "zookeeper/hadoop."+KerberosUtil.getKrb5DomainRealm().toLowerCase();
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);

        // clean control files before job submit
        MultiComponentExample.cleanupBeforeRun();

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
        config.addResource("hive-site.xml");
        // add hbase config file
        Configuration conf = HBaseConfiguration.create(config);
        Utils.handleZkSslEnabled(conf);

        // Initialize the job object.
        Job job = Job.getInstance(conf);
        job.setJarByClass(MultiComponentExample.class);

        // set mapper&reducer class
        job.setMapperClass(MultiComponentMapper.class);
        job.setReducerClass(MultiComponentReducer.class);

        // set job input&output
        FileInputFormat.addInputPath(job, new Path(baseDir, INPUT_DIR_NAME + File.separator + "data.txt"));
        FileOutputFormat.setOutputPath(job, new Path(baseDir, OUTPUT_DIR_NAME));

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
        String tokenStr = ((HiveConnection) connection)
                .getDelegationToken(UserGroupInformation.getCurrentUser().getShortUserName(), PRINCIPAL);
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
