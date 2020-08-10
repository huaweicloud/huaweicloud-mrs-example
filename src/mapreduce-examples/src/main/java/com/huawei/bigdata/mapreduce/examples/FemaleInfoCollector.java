package com.huawei.bigdata.mapreduce.examples;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.huawei.hadoop.security.LoginUtil;

/**
 * MR instance
 */
public class FemaleInfoCollector {
	
  /**
   * Mapper class
   */
  public static class CollectionMapper extends
      Mapper<Object, Text, Text, IntWritable> {

    // Delimiter
    String delim;

    // Filter sex.
    String sexFilter;

    // Name
    private Text nameInfo = new Text();

    // Output <key,value> must be serialized.
    private IntWritable timeInfo = new IntWritable(1);

    /**
     * Distributed computing
     *
     * @param key     Object : location offset of the source file
     * @param value   Text : a row of characters in the source file
     * @param context Context : output parameter
     * @throws IOException , InterruptedException
     */
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      String line = value.toString();

      if (line.contains(sexFilter)) {

        // A character string that has been read
        String name = line.substring(0, line.indexOf(delim));
        nameInfo.set(name);
        // Obtain the dwell duration.
        String time = line.substring(line.lastIndexOf(delim) + 1,
            line.length());
        timeInfo.set(Integer.parseInt(time));

        // The Map task outputs a key-value pair.
        context.write(nameInfo, timeInfo);
      }
    }

    /**
     * map use to init
     *
     * @param context Context
     */
    public void setup(Context context) throws IOException,
        InterruptedException {

      // Obtain configuration information using Context.
      delim = context.getConfiguration().get("log.delimiter", ",");

      sexFilter = delim
          + context.getConfiguration()
          .get("log.sex.filter", "female") + delim;
    }
  }

  /**
   * Reducer class
   */
  public static class CollectionReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {

    // Statistical results
    private IntWritable result = new IntWritable();

    // Total time threshold
    private int timeThreshold;

    /**
     * @param key     Text : key after Mapper
     * @param values  Iterable : all statistical results with the same key
     * @param context Context
     * @throws IOException , InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException,
                       InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      // No results are output if the time is less than the threshold.
      if (sum < timeThreshold) {
        return;
      }
      result.set(sum);

      // In the output information, key indicates netizen information,
      // and value indicates the total online time of the netizen.
      context.write(key, result);
    }


    /**
     * The setup() method is invoked for only once before the
     * map() method or reduce() method.
     *
     * @param context Context
     * @throws IOException , InterruptedException
     */
    public void setup(Context context) throws IOException,
        InterruptedException {

      // Context obtains configuration information.
      timeThreshold = context.getConfiguration().getInt(
          "log.time.threshold", 120);
    }
  }

  /**
   * Combiner class
   */
  public static class CollectionCombiner extends
  Reducer<Text, IntWritable, Text, IntWritable> {

	  // Intermediate statistical results
	  private IntWritable intermediateResult = new IntWritable();

	  /**
	   * @param key     Text : key after Mapper
	   * @param values
	   *     Iterable : all results with the same key in this map task
	   * @param context Context
	   * @throws IOException , InterruptedException
	   */
	  public void reduce(Text key, Iterable<IntWritable> values,
			  Context context) throws IOException, InterruptedException {
		  int sum = 0;
		  for (IntWritable val : values) {
			  sum += val.get();
		  }

		  intermediateResult.set(sum);

		  // In the output information, key indicates netizen information,
		  // and value indicates the total online time
		  // of the netizen in this map task.
		  context.write(key, intermediateResult);
	  }

  }


  /**
   * main function
   *
   * @param args String[] :
   *     index 0:process file directory  .
   *     index 1:process out file directory
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    // Initialize environment variables.
    Configuration conf = new Configuration();

    // Obtain input parameters.
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: FemaleInfoCollector <in> <out>");
      System.exit(2);
    }

	// Judge whether the security mode
	if("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication"))){
  	    //security mode

        /**
         * 用户自己申请的机机账号名称
         */
        final String PRINCIPAL= "test@HADOOP.COM";

        /**
         * 用户自己申请的机机账号keytab文件名称
         */
        final String USER_KEYTAB_FILE = "user.keytab";
        String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        String krbFile = filePath + "krb5.conf";
        String userKeyTableFile = filePath + USER_KEYTAB_FILE;
	   	System.setProperty("java.security.krb5.conf", krbFile);
	    LoginUtil.login(PRINCIPAL, userKeyTableFile, krbFile, conf);
	}
    
    // Initialize the job object.
    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "Collect Female Info");
    job.setJarByClass(FemaleInfoCollector.class);

    // Set map and reduce classes to be executed,
    // or specify the map and reduce classes using configuration files.
    job.setMapperClass(CollectionMapper.class);
    job.setReducerClass(CollectionReducer.class);

    // Set the Combiner class. The combiner class is not used by default.
    // In general, combiner class can be set same as the reduce class.
    // In this scenario, the Reducer will filter
    // some records, so we need to create a new combiner class.
    // Exercise caution when using the Combiner class.
    // You can specify it using configuration files.
    job.setCombinerClass(CollectionCombiner.class);

    // Set the output type of the job.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    // Submit the job to a remote environment for execution.
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}
