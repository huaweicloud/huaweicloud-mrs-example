/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * IoTDB Session Pool Class
 *
 * @since 2022-01-14
 */
public class IoTDBSessionPool {
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBSessionPool.class);

    private static SessionPool pool;

    public IoTDBSessionPool(String host, int port, String username, String password, int size) {
        pool = new SessionPool(host, port, username, password, size);
    }

    // more insert example, see SessionExample.java
    public void insertRecord(String kafkaData) {
        // parse kafka data
        String[] data = kafkaData.split(",");

        String deviceId = Constant.ROOT_VEHICLE_DEVICEID;
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        measurements.add(data[0]);
        types.add(TSDataType.FLOAT);
        values.add(Float.parseFloat(data[2]));
        try {
            pool.insertRecord(deviceId, Long.parseLong(data[1]), measurements, types, values);
        } catch (IoTDBConnectionException | StatementExecutionException | TTransportException e) {
            LOG.error("Insert data failed.", e);
        }
    }

}
