/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import com.huawei.hadoop.security.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseMultiClusterConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * HBase Development Instruction Sample Code. The sample code uses user information as source data,
 * it introduces how to implement business process development using HBase API when using double-read feature
 *
 * @since 2023-08-20
 */
public class HBaseDualReadSample extends HBaseSample {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseDualReadSample.class.getName());

    private static final String CONNECTION_IMPL_KEY = "hbase.client.connection.impl";

    private static final String DUAL_READ_CONNECTION = "org.apache.hadoop.hbase.client.HBaseMultiClusterConnectionImpl";

    private static final String HBASE_DUAL_XML_NAME = "hbase-dual.xml";

    private static final String ACTIVE_DIRECTORY = "active";

    private static final String STANDBY_DIRECTORY = "standby";

    // True means that we use hbase-dual.xml configuration scheme to set the active/standby cluster configuration item.
    // False means that we use configuration transfer solution which we need to manually set the configuration
    // of the active/standby cluster.
    private static final boolean IS_CREATE_CONNECTION_BY_XML = true;

    /**
     * Constructor for {@link HBaseDualReadSample}
     *
     * @throws IOException Creating sample exception
     */
    public HBaseDualReadSample() throws IOException {
        super();
        setHbaseDualReadParam();
    }

    private void setHbaseDualReadParam() {
        // We need to set the conf of the active/standby cluster to null to avoid the impact of the previous test.
        HBaseMultiClusterConnection.setActiveConf(null);
        HBaseMultiClusterConnection.setStandbyConf(null);
        if (IS_CREATE_CONNECTION_BY_XML) {
            // In Windows environment
            String userDir =
                HBaseDualReadSample.class.getClassLoader().getResource(Utils.CONF_DIRECTORY).getPath() + File.separator;
            // In Linux environment
            // String userDir = System.getProperty("user.dir") + File.separator + Utils.CONF_DIRECTORY + File.separator;
            clientConf.addResource(new Path(userDir + HBASE_DUAL_XML_NAME), false);
        } else {
            addHbaseDualXmlParam(clientConf);
            initActiveConf();
            initStandbyConf();
        }
    }

    private void addHbaseDualXmlParam(Configuration conf) {
        // We need to set the optional parameters contained in hbase-dual.xml to conf
        // when we use configuration transfer solution
        conf.set(CONNECTION_IMPL_KEY, DUAL_READ_CONNECTION);
        // conf.set("", "");
    }

    private void initActiveConf() {
        // The hbase-dual.xml configuration scheme is used to generate the client configuration of the active cluster.
        // In actual application development, you need to generate the client configuration of the active cluster.
        String activeDir = HBaseDualReadSample.class.getClassLoader().getResource(Utils.CONF_DIRECTORY).getPath()
            + File.separator + ACTIVE_DIRECTORY + File.separator;
        Configuration activeConf = Utils.createConfByUserDir(activeDir);
        HBaseMultiClusterConnection.setActiveConf(activeConf);
    }

    private void initStandbyConf() {
        // The hbase-dual.xml configuration scheme is used to generate the client configuration of the standby cluster.
        // In actual application development, you need to generate the client configuration of the standby cluster.
        String standbyDir = HBaseDualReadSample.class.getClassLoader().getResource(Utils.CONF_DIRECTORY).getPath()
            + File.separator + STANDBY_DIRECTORY + File.separator;
        Configuration standbyConf = Utils.createConfByUserDir(standbyDir);
        HBaseMultiClusterConnection.setStandbyConf(standbyConf);
    }
}
