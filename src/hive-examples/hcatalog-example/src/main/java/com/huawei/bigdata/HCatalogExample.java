/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package com.huawei.bigdata;

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
 * HCatalogExample
 *
 * @author
 * @since 8.0.0
 */
public class HCatalogExample extends Configured implements Tool {
    /**
     * Map process
     */

    public static class Map extends
            Mapper<LongWritable, HCatRecord, IntWritable, IntWritable> {
        int age;

        @Override
        protected void map(
                LongWritable key,
                HCatRecord value,
                Mapper<LongWritable, HCatRecord,
                        IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            if ( value.get(0) instanceof Integer ) {
                age = (Integer) value.get(0);
            }
            context.write(new IntWritable(age), new IntWritable(1));
        }
    }

    /**
     * Reduce process
     */
    public static class Reduce extends Reducer<IntWritable, IntWritable,
            IntWritable, HCatRecord> {
        @Override
        protected void reduce(
                IntWritable key,
                Iterable<IntWritable> values,
                Reducer<IntWritable, IntWritable,
                        IntWritable, HCatRecord>.Context context)
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

    /**
     * Run method
     */
    public int run(String[] args) throws Exception {
        HiveConf.setLoadMetastoreConfig(true);
        Configuration conf = getConf();
        String[] otherArgs = args;
        String dbName = "default";
        String inputTableName = otherArgs[0];

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

        String outputTableName = otherArgs[1];

        OutputJobInfo outputjobInfo = OutputJobInfo.create(dbName, outputTableName, null);
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