/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import org.apache.iotdb.flink.IoTDBSink;
import org.apache.iotdb.flink.IoTSerializationSchema;
import org.apache.iotdb.flink.options.IoTDBSinkOptions;

/**
 * The IoTDBSource
 *
 * @since 2023/3/25
 */
public class IoTDBSinkSSL<T> extends IoTDBSink<T> {
    private static IoTDBProperties iotdbProps = IoTDBProperties.getInstance();
    /**
     * set truststore.jks path only when iotdb_ssl_enable is true.
     * if modify iotdb_ssl_enable to false, modify IoTDB client's iotdb_ssl_enable="false" in iotdb-client.env,
     * iotdb-client.env file path: /opt/client/IoTDB/iotdb/conf
     */
    public static String IOTDB_SSL_ENABLE = iotdbProps.getIotdb_ssl_enable();
    public static String IOTDB_SSL_TRUSTSTORE = iotdbProps.getIotdb_ssl_truststore();

    static {
        // set iotdb_ssl_enable
        System.setProperty("iotdb_ssl_enable", IOTDB_SSL_ENABLE);
        if ("true".equals(IOTDB_SSL_ENABLE)) {
            // set truststore.jks path
            System.setProperty("iotdb_ssl_truststore", IOTDB_SSL_TRUSTSTORE);
        }
    }

    public IoTDBSinkSSL(IoTDBSinkOptions options, IoTSerializationSchema<T> schema) {
        super(options, schema);
    }
}
