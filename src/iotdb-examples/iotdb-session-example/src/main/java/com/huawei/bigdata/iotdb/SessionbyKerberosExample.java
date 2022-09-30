/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import com.huawei.iotdb.client.security.IoTDBClientKerberosFactory;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.ietf.jgss.GSSException;

import java.util.Base64;

import javax.security.auth.login.LoginException;

public class SessionbyKerberosExample {
    private static Session session;
    private static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
    private static final String ROOT_SG1_D1_S2 = "root.sg1.d1.s2";
    private static final String ROOT_SG1_D1_S3 = "root.sg1.d1.s3";
    private static final String HOST = "127.0.0.1";
    private static final String PORT = "22260";

    private static final String JAVA_KRB5_CONF = "java.security.krb5.conf";
    /**
     * Location of krb5.conf file
     */
    private static final String KRB5_CONF_DEST = "下载的认证凭据中“krb5.conf”文件的位置";
    /**
     * Location of keytab file
     */
    private static final String KEY_TAB_DEST = "下载的认证凭据中“user.keytab”文件的位置";
    /**
     * User principal
     */
    private static final String CLIENT_PRINCIPAL = "对应user.keytab的用户名@HADOOP.COM";

    /**
     * Server principal, 'iotdb_server_kerberos_principal' in iotdb-datanode.properties
     */
    private static final String SERVER_PRINCIPAL = "iotdb/hadoop.hadoop.com@HADOOP.COM";

    /**
     * set truststore.jks path only when iotdb_ssl_enable is true.
     * if modify iotdb_ssl_enable to false, modify IoTDB client's iotdb_ssl_enable="false" in iotdb-client.env,
     * iotdb-client.env file path: /opt/client/IoTDB/iotdb/conf
     */
    private static final String IOTDB_SSL_ENABLE = "true";

    /**
     * Get kerberos token as password
     * @return kerberos token
     * @throws LoginException loginException
     * @throws GSSException GSSException
     */
    public static String getAuthToken() throws LoginException, GSSException {
        IoTDBClientKerberosFactory kerberosHandler = IoTDBClientKerberosFactory.getInstance();
        System.setProperty(JAVA_KRB5_CONF, KRB5_CONF_DEST);
        kerberosHandler.loginSubjectFromKeytab(CLIENT_PRINCIPAL, KEY_TAB_DEST);
        byte[] tokens = kerberosHandler.generateServiceToken(SERVER_PRINCIPAL);
        return Base64.getEncoder().encodeToString(tokens);
    }


    public static void main(String[] args)
        throws IoTDBConnectionException, StatementExecutionException, GSSException, LoginException {
        // set iotdb_ssl_enable
        System.setProperty("iotdb_ssl_enable", IOTDB_SSL_ENABLE);
        if ("true".equals(IOTDB_SSL_ENABLE)) {
            // set truststore.jks path
            System.setProperty("iotdb_ssl_truststore", "truststore文件路径");
        }

        session = new Session(HOST, PORT,"认证用户名称", getAuthToken());
        session.open(false);

        // set session fetchSize
        session.setFetchSize(10000);

        try {
            session.setStorageGroup("root.sg1");
        } catch (StatementExecutionException e) {
            if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
                throw e;
            }
        }
        createTimeseries();
    }

    private static void createTimeseries() throws IoTDBConnectionException, StatementExecutionException {
        if (!session.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
            session.createTimeseries(
                ROOT_SG1_D1_S1, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
        }
        if (!session.checkTimeseriesExists(ROOT_SG1_D1_S2)) {
            session.createTimeseries(
                ROOT_SG1_D1_S2, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
        }
        if (!session.checkTimeseriesExists(ROOT_SG1_D1_S3)) {
            session.createTimeseries(
                ROOT_SG1_D1_S3, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
        }
    }
}
