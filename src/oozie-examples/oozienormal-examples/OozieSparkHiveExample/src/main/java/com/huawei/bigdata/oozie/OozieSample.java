/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.oozie;

import com.huawei.bigdata.utils.Helper;
import com.huawei.bigdata.utils.PropertiesCache;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Oozie Sample
 *
 * @since 2020-09-30
 */
public class OozieSample {
    private static final Logger logger = LoggerFactory.getLogger(OozieSample.class);

    private static String JOB_PROPERTIES_FILE = "job.properties";

    private String userConfDir = null;

    private OozieClient wc = null;

    public OozieSample() {
        userConfDir = Helper.getResourcesPath();
        wc = new OozieClient(PropertiesCache.getInstance().getProperty("oozie_url_default"));
    }

    /**
     * Oozie test method
     *
     * @param jobFilePath job.properties file path
     *
     * @since 2020-09-30
     */
    public void test(String jobFilePath) {
        try {
            runJob(jobFilePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runJob(String jobFilePath) throws OozieClientException, InterruptedException {

        Properties conf = getJobProperties(jobFilePath);
        String user = PropertiesCache.getInstance().getProperty("submit_user");
        conf.setProperty("user.name", user);

        // submit and start the workflow job
        String jobId = wc.run(conf);

        logger.info(" Workflow job submitted: {}" , jobId);

        // wait until the workflow job finishes printing the status every 10 seconds
        while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            logger.info("Workflow job running ... {}" , jobId);
            Thread.sleep(10 * 1000);
        }

        // print the final status of the workflow job
        logger.info("Workflow job completed ... {}" , jobId);
        logger.info(String.valueOf(wc.getJobInfo(jobId)));
    }

    /**
     * Get job.properties File in filePath
     *
     * @param filePath file path
     * @return job.properties
     * @since 2020-09-30
     */
    public Properties getJobProperties(String filePath) {
        File configFile = new File(filePath);
        if (!configFile.exists()) {
            logger.info(filePath , "{} is not exist.");
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
            logger.info("create a new job : {}" , jobId);

            if (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED) {
                logger.info("you should not kill a done job : {}" , jobId);
                return;
            }

            wc.kill(jobId);

            WorkflowJob.Status currentStatus = wc.getJobInfo(jobId).getStatus();
            if (currentStatus == WorkflowJob.Status.KILLED) {
                logger.info("it's success to kill the job : {}" , jobId);
            } else {
                logger.info("failed to kill the job : {}, the current status is {}" , jobId , currentStatus);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
