package com.huawei.bigdata.spark.examples;

import org.apache.spark.launcher.SparkLauncher;

/**
  * Submit spark app.
  * args(0) is the mode to run spark app, eg yarn-client
  * args(1) is the path of spark app jar
  * args(2) is the main class of spark app
  * args(3...) is the parameters of spark app
  */
public class SparkLauncherExample {
    public static void main(String[] args) throws Exception {
        System.out.println("com.huawei.bigdata.spark.examples.SparkLauncherExample <mode> <jarParh> <app_main_class> <appArgs>");
        SparkLauncher launcher = new SparkLauncher();
        launcher.setMaster(args[0])
            .setAppResource(args[1]) // Specify user app jar path
            .setMainClass(args[2]);
        if (args.length > 3) {
            String[] list = new String[args.length - 3];
            for (int i = 3; i < args.length; i++) {
                list[i-3] = args[i];
            }
            // Set app args
            launcher.addAppArgs(list);
        }

        // Launch the app
        Process process = launcher.launch();
        // Get Spark driver log
        new Thread(new ISRRunnable(process.getErrorStream())).start();
        int exitCode = process.waitFor();
        System.out.println("Finished! Exit code is "  + exitCode);
    }
}

