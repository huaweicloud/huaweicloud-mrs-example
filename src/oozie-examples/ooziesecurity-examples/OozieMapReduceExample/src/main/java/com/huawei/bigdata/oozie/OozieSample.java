/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.oozie;

import com.huawei.bigdata.utils.Helper;
import com.huawei.bigdata.utils.PropertiesCache;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(OozieSample.class);

    private static String JOB_PROPERTIES_FILE = "job.properties";

    private OozieClient wc = null;

    private String userConfDir = null;

    public OozieSample() {
        userConfDir = Helper.getResourcesPath();
        wc = new AuthOozieClient(PropertiesCache.getInstance().getProperty("oozie_url_default"),AuthOozieClient.AuthType.KERBEROS.name());
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
            UserGroupInformation.getLoginUser()
                    .doAs(
                            new PrivilegedExceptionAction<Void>() {
                                /**
                                 * run job
                                 *
                                 * @return null
                                 * @throws Exception exception
                                 */
                                public Void run() throws Exception {
                                    runJob(jobFilePath);
                                    return null;
                                }
                            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runJob(String jobFilePath) throws OozieClientException, InterruptedException {

        Properties conf = getJobProperties(jobFilePath);

        // submit and start the workflow job
        String jobId = wc.run(conf);

        logger.info("Workflow job  submitted: {}" , jobId);

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
                } catch (IOException exception) {
                    exception.printStackTrace();
                }
            }
        }

        return properties;
    }
}
