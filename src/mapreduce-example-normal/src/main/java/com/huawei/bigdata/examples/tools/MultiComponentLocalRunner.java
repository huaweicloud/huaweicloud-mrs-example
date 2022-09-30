package com.huawei.bigdata.examples.tools;

import com.huawei.bigdata.examples.util.JarFinderUtil;
import com.huawei.bigdata.mapreduce.examples.MultiComponentExample;
import com.huawei.bigdata.mapreduce.examples.MultiComponentExample.MultiComponentMapper;
import com.huawei.bigdata.mapreduce.examples.MultiComponentExample.MultiComponentReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;


public class MultiComponentLocalRunner {
    private static void cleanupBeforeRun(Configuration config) throws IOException {
        FileSystem tempFS = FileSystem.get(config);
        tempFS.delete(new Path(MultiComponentExample.baseDir, MultiComponentExample.OUTPUT_DIR_NAME), true);
    }

    public static void main(String[] args) throws Exception {
        //set user with input dir owner
        System.setProperty("HADOOP_USER_NAME", "root");

        // Create JAR
        TarManager.createJar();
        Configuration config = new Configuration();


        String hiveClientProperties = MultiComponentLocalRunner.class.getClassLoader()
                .getResource("hiveclient.properties").getPath();
        String hbaseSite = MultiComponentLocalRunner.class.getClassLoader().getResource("hbase-site.xml").getPath();
        String hiveSite = MultiComponentLocalRunner.class.getClassLoader().getResource("hive-site.xml").getPath();

        // a list of files, separated by comma
        String files = "file://" + hiveClientProperties;
        files = files + "," + "file://" + hiveSite;
        files = files + "," + "file://" + hbaseSite;

        // this setting will ask Job upload these files to HDFS

        config.set("tmpfiles", files);

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
        config.addResource(MultiComponentExample.class.getClassLoader().getResourceAsStream("hive-site.xml"));
        // add hbase config file
        Configuration conf = HBaseConfiguration.create(config);

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

        // Submit the job to a remote environment for execution.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
