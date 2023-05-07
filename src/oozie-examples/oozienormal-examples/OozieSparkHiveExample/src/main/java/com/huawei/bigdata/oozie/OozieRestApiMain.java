/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.oozie;

import com.huawei.bigdata.utils.Helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oozie Example for normal Mode using Java API
 *
 * @since 2021-01-25
 */
public class OozieRestApiMain {
    private static final Logger logger = LoggerFactory.getLogger(OozieRestApiMain.class);

    public static void main(String[] args) {
        try {
            String resourcesPathpath = Helper.getResourcesPath();

            String jobFilePath = resourcesPathpath + "job.properties";

            new OozieSample().test(jobFilePath);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        logger.info("-----------finish Oozie -------------------");
    }
}
