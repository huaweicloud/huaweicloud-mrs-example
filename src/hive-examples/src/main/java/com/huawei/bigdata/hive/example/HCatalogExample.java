package com.huawei.bigdata.hive.example;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

/**
 * 概述：
 * 1)本示例演示了如何使用HCatalog提供的HCatInputFormat和HCatOutputFormat接口。
 * 2)本示例将演示从Hive表t1读取数据，并进行group by后再 load到表t2的详细实现细节,相当于以下HQL:
 *         select col1, count(*) from t1 group by col1;
 *
 * 使用前准备：
 * 1)创建源头表t1:
 *    create table t1(col1 int);
 * 2)创建目的表t2:
 *         create table t2(col1 int,col2 int);
 * 3)将工程打成jar包后上传。
 * 4)配置环境变量：
 *
 *  export HADOOP_HOME=<path_to_hadoop_install>
 *  export HCAT_HOME=<path_to_hcat_install>
 *  export HIVE_HOME=<path_to_hive_install>
 *  export LIB_JARS=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-1.3.0.jar,
 *         $HCAT_HOME/lib/hive-metastore-1.3.0.jar,
 *         $HCAT_HOME/lib/hive-exec-1.3.0.jar,
 *         $HCAT_HOME/lib/libfb303-0.9.2.jar,
 *         $HCAT_HOME/lib/slf4j-api-1.7.5.jar

 *  export HADOOP_CLASSPATH=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-1.3.0.jar:
 *         $HCAT_HOME/lib/hive-metastore-1.3.0.jar:$HIVE_HOME/lib/hive-exec-1.3.0.jar:
 *         $HCAT_HOME/lib/libfb303-0.9.2.jar:
 *         $HADOOP_HOME/etc/hadoop:$HIVE_HOME/conf:
 *         $HCAT_HOME/lib/slf4j-api-1.7.5.jar
 *
 * 提交任务：
 *         yarn --config $HADOOP_HOME/etc/hadoop jar jar <path_to_jar> <main_class> -libjars $LIB_JARS t1 t2
 *
 * 参考资料：
 * 详细接口说明请参考：https://cwiki.apache.org/confluence/display/Hive/HCatalog+InputOutput
 *
 * */

public class HCatalogExample extends Configured implements Tool {

    public static class Map extends
            Mapper<LongWritable, HCatRecord, IntWritable, IntWritable> {
        int age;
        @Override
        protected void map(
                LongWritable key,
                HCatRecord value,
                Context context)
                throws IOException, InterruptedException {
            age = (Integer) value.get(0);
            context.write(new IntWritable(age), new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable,
    IntWritable, HCatRecord> {
      @Override
      protected void reduce(
              IntWritable key,
              Iterable<IntWritable> values,
              Context context)
              throws IOException, InterruptedException {
          int sum = 0;
          Iterator<IntWritable> iter = values.iterator();
          while (iter.hasNext()) {
              sum++;
              iter.next();
          }
          HCatRecord record = new DefaultHCatRecord(2);
          record.set(0, key.get());
          record.set(1, sum);

          context.write(null, record);
        }
    }

    public int run(String[] args) throws Exception {

        HiveConf.setLoadMetastoreConfig(true);
        Configuration conf = getConf();
        String[] otherArgs = args;

        String inputTableName = otherArgs[0];
        String outputTableName = otherArgs[1];
        String dbName = "default";

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "GroupByDemo");

        HCatInputFormat.setInput(job, dbName, inputTableName);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setJarByClass(HCatalogExample.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);

        OutputJobInfo outputjobInfo = OutputJobInfo.create(dbName,outputTableName, null);
        HCatOutputFormat.setOutput(job, outputjobInfo);
        HCatSchema schema = outputjobInfo.getOutputSchema();
        HCatOutputFormat.setSchema(job, schema);
        job.setOutputFormatClass(HCatOutputFormat.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HCatalogExample(), args);
        System.exit(exitCode);
    }
}