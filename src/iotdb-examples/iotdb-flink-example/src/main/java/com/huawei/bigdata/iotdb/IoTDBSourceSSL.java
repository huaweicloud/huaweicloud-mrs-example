/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import static com.huawei.bigdata.iotdb.FlinkIoTDBSource.IOTDB_SSL_ENABLE;

import org.apache.iotdb.flink.IoTDBSource;
import org.apache.iotdb.flink.options.IoTDBSourceOptions;

/**
 * The IoTDBSource
 *
 * @since 2023/3/25
 */
public abstract class IoTDBSourceSSL<T> extends IoTDBSource<T> {
    static {
        // set iotdb_ssl_enable
        System.setProperty("iotdb_ssl_enable", IOTDB_SSL_ENABLE);
        if ("true".equals(IOTDB_SSL_ENABLE)) {
            // set truststore.jks path
            System.setProperty("iotdb_ssl_truststore", "truststore文件路径");
        }
    }

    protected IoTDBSourceSSL(IoTDBSourceOptions ioTDBSourceOptions) {
        super(ioTDBSourceOptions);
    }
}
