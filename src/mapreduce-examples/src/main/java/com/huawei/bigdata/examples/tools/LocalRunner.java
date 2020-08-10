package com.huawei.bigdata.examples.tools;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.huawei.bigdata.mapreduce.examples.FemaleInfoCollector;
import com.huawei.bigdata.mapreduce.examples.FemaleInfoCollector.CollectionCombiner;
import com.huawei.bigdata.mapreduce.examples.FemaleInfoCollector.CollectionMapper;
import com.huawei.bigdata.mapreduce.examples.FemaleInfoCollector.CollectionReducer;

/**
 * run MR application
 */
public class LocalRunner {

  /**
   * main detail:
   * 1. produce JAR, CreateJar method use to create JAR
   * 2. initialize Env, load configuration files and set parameters.
   * 3. upload source files to be handled.
   * 4. submit MR job.
   * @param args String[] :
   *   index 0:process file directory
   *   index 1:process out file directory
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    // Create JAR
    TarManager.createJar();

    // Init environment, load xml file
    Configuration conf = getConfiguration();

    // The native source file path
    String filelist = conf.get("username.client.filelist.conf");

    //Input file path, the first parameter
    String inputPath = conf.get("username.client.mapred.input");

    // MR output path,the second parameter
    String outputPath = conf.get("username.client.mapred.output");

    // If set input parameter, use the input parameters,
    // else use default parameter
    String[] parArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();

    if (parArgs.length >= 1) {
      inputPath = parArgs[0];
    }

    if (parArgs.length >= 2) {
      outputPath = parArgs[1];
    }

    // Get current path
    String dir = System.getProperty("user.dir");

    // Local directory
    String localPath = dir + File.separator + "data";

    // Put source files to server
    FileSystem fileSystem = FileSystem.get(conf);
    uploadFiles(fileSystem, filelist,
        localPath, inputPath);

    // Delete output path's files
    if (fileSystem.exists(new Path(outputPath))) {
      fileSystem.delete(new Path(outputPath), true);
    }

    // Initialize the job object.
    Job job = Job.getInstance(conf, "Collect Female Info");

    // Set excute jar and class
    job.setJar(dir + File.separator + "mapreduce-examples.jar");
    job.setJarByClass(FemaleInfoCollector.class);

    // Set map and reduce classes to be executed, or specify the map
    // and reduce classes using configuration files.
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

    // Set hdfs input path and, output path
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    // Submit the job to a remote environment for execution.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * get conf object
   *
   * @return Configuration
   */
  public static Configuration getConfiguration() {
    // Default load from conf directory
    Configuration conf = new Configuration();
    conf.addResource(LocalRunner.class.getClassLoader().
    		getResourceAsStream("core-site.xml"));
    conf.addResource(LocalRunner.class.getClassLoader().
    		getResourceAsStream("yarn-site.xml"));
    conf.addResource(LocalRunner.class.getClassLoader().
    		getResourceAsStream("mapred-site.xml"));
    conf.addResource(LocalRunner.class.getClassLoader().
    		getResourceAsStream("hdfs-site.xml"));
    conf.addResource(LocalRunner.class.getClassLoader().
    		getResourceAsStream("user-mapred.xml"));
    return conf;
  }

  /**
   * upload local files to remote file system
   *
   * @param fileSystem FileSystem : file system
   * @param fileConfig String : conf files to be upload
   * @param localPath  String : local file directory
   * @param inputPath  String : remote target path
   * @return boolean : result
   */
  private static void uploadFiles(FileSystem fileSystem,
                                 final String fileConfig,
                                 final String localPath,
                                 final String inputPath) throws Exception {

    // local files which are to be uploaded
    String[] filenames = fileConfig.split(",");

    if (filenames == null || filenames.length <= 0) {
      throw new Exception("The files to be uploaded are not specified.");
    }

    // file loader to hdfs
    FileUploader fileLoader = null;

    for (int i = 0; i < filenames.length; i++) {
      if (filenames[i] == null || "".equals(filenames[i])) {
        continue;
      }

      // Excute upload hdfs
      fileLoader = new FileUploader(fileSystem, inputPath,
          filenames[i], localPath + File.separator + filenames[i]);
      fileLoader.upload();
    }
  }
}
