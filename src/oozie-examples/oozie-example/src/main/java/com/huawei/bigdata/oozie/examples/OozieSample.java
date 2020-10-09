/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.oozie.examples;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

/**
 * Oozie Sample
 *
 * @since 2020-09-30
 */
public class OozieSample {
    private static String JOB_PROPERTIES_FILE = "job.properties";

    private String userConfDir = null;

    private boolean isSecury;

    private OozieClient wc = null;

    public OozieSample(boolean isSecurityCluster) throws IOException {
        userConfDir = Constant.APPLICATION_PATH;
        this.isSecury = isSecurityCluster;
        wc = new OozieClient(Constant.OOZIE_URL_DEFALUT);
    }

    /**
     * Oozie test method
     *
     * @since 2020-09-30
     */
    public void test() throws Exception {
        try {
            System.out.println("cluset status is " + isSecury);
            if (isSecury) {
                UserGroupInformation.getLoginUser()
                        .doAs(
                                new PrivilegedExceptionAction<Void>() {
                                    public Void run() throws Exception {
                                        runJob();
                                        return null;
                                    }
                                });
            } else {
                runJob();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runJob() throws OozieClientException, InterruptedException {
        String mrJobFilePath = userConfDir + JOB_PROPERTIES_FILE;

        Properties conf = getJobProperties(mrJobFilePath);

        // submit and start the workflow job
        String jobId = wc.run(conf);

        System.out.println("Workflow job submitted: " + jobId);

        // wait until the workflow job finishes printing the status every 10 seconds
        while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("Workflow job running ..." + jobId);
            Thread.sleep(10 * 1000);
        }

        // print the final status of the workflow job
        System.out.println("Workflow job completed ..." + jobId);
        System.out.println(wc.getJobInfo(jobId));
    }

    /**
     * Get job.properties File in filePath
     *
     * @since 2020-09-30
     */
    public Properties getJobProperties(String filePath) {
        File configFile = new File(filePath);
        if (!configFile.exists()) {
            System.out.println(filePath + " is not exist.");
        }

        InputStream inputStream = null;

        // create a workflow job configuration
        Properties properties = wc.createConfiguration();
        try {
            inputStream = new FileInputStream(filePath);
            properties.load(inputStream);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return properties;
    }

    private void killJob() {
        try {
            String mrJobFilePath = userConfDir + JOB_PROPERTIES_FILE;

            Properties conf = getJobProperties(mrJobFilePath);

            // submit and start the workflow job
            String jobId = wc.run(conf);
            System.out.println("create a new job : " + jobId);

            if (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED) {
                System.out.println("you should not kill a done job : " + jobId);
                return;
            }

            wc.kill(jobId);

            WorkflowJob.Status currentStatus = wc.getJobInfo(jobId).getStatus();
            if (currentStatus == WorkflowJob.Status.KILLED) {
                System.out.println("it's success to kill the job : " + jobId);
            } else {
                System.out.println("failed to kill the job : " + jobId + ", the current status is " + currentStatus);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
