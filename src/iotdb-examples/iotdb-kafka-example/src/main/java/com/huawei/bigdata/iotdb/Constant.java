/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

/**
 * IoTDB Session Pool Class
 *
 * @since 2022-01-14
 */
public class Constant {
    /**
     * 用户自己申请的机机账号keytab文件名称
     */
    public static final String USER_KEYTAB_FILE = "用户自己申请的机机账号keytab文件名称";

    /**
     * 用户自己申请的机机账号名称
     */
    public static final String USER_PRINCIPAL = "用户自己申请的机机账号名称";

    /**
     * IoTDB 数据模板：<传感器名称，时间戳，值>
     */
    public static final String IOTDB_DATA_SAMPLE_TEMPLATE = "sensor_%d,%d,%f";

    /**
     * IoTDB 元数据
     */
    public static final String ROOT_VEHICLE_DEVICEID = "root.vehicle.deviceid";

    public final static String BOOTSTRAP_SERVER = "bootstrap.servers";

    public final static String GROUP_ID = "group.id";

    public final static String VALUE_DESERIALIZER = "value.deserializer";

    public final static String KEY_DESERIALIZER = "key.deserializer";

    public final static String SECURITY_PROTOCOL = "security.protocol";

    public final static String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";

    public final static String KERBEROS_DOMAIN_NAME = "kerberos.domain.name";

    public final static String ENABLE_AUTO_COMMIT = "enable.auto.commit";

    public final static String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";

    public final static String SESSION_TIMEOUT_MS = "session.timeout.ms";

    public final static String CLIENT_ID = "client.id";

    public final static String KEY_SERIALIZER = "key.serializer";

    public final static String VALUE_SERIALIZER = "value.serializer";
}