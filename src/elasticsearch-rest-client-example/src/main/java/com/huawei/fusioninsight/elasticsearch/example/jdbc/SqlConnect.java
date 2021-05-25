/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.jdbc;

import com.amazon.opendistroforelasticsearch.jdbc.config.KeyTabConnectionProperty;
import com.amazon.opendistroforelasticsearch.jdbc.config.PrincipalConnectionProperty;
import com.amazon.opendistroforelasticsearch.jdbc.config.SecurityEnableConnectionProperty;
import com.amazon.opendistroforelasticsearch.jdbc.config.UseSSLConnectionProperty;
import com.amazon.opendistroforelasticsearch.jdbc.config.Krb5ConfConnectionProperty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 初始化connector
 *
 * @since 2020-11-5
 */
public class SqlConnect {
    private static final Logger LOG = LogManager.getLogger(SqlConnect.class);
    private final Properties properties = new Properties();
    private String sslEnable;
    private String esServerHost;

    /**
     * 初始化connection
     *
     * @return connection
     */
    public Connection sqlConnect() {
        Properties properties = initConfig();
        if (properties == null) {
            LOG.error("Get properties error");
            return null;
        }
        try {
            String url = getUrl();
            if (url == null) {
                LOG.error("Get server url error");
                return null;
            }
            return DriverManager.getConnection(url, properties);
        } catch (SQLException e) {
            LOG.error("Connect error.");
            return null;
        }
    }

    private Properties initConfig() {
        String configPath;
        String esUser;
        String keytabPth;
        String krb5Path;
        String securityEnable;
        String userPath = System.getProperty("user.dir");
        if (userPath == null) {
            LOG.error("Failed to load user path.");
            return null;
        }
        configPath = userPath + File.separator + "conf" + File.separator;
        String path = configPath + "esParams.properties";
        try {
	        properties.load(new FileInputStream(new File(path)));
        } catch (IOException e) {
            LOG.error("Failed to load properties file: {} ", path);
            return null;
        }
        esUser = properties.getProperty("principal");
        sslEnable = properties.getProperty("sslEnabled");
        securityEnable = properties.getProperty("isSecureMode");
        esServerHost = properties.getProperty("esServerHost");
        keytabPth = configPath + "user.keytab";
        krb5Path = configPath + "krb5.conf";
        Properties properties = new Properties();
        properties.put(UseSSLConnectionProperty.KEY, sslEnable);
        properties.put(SecurityEnableConnectionProperty.KEY, securityEnable);
        properties.put(PrincipalConnectionProperty.KEY, esUser);
        properties.put(KeyTabConnectionProperty.KEY, keytabPth);
        properties.put(Krb5ConfConnectionProperty.KEY, krb5Path);
        return properties;
    }

    private String getUrl() {
        String[] esHostList = esServerHost.split(",");
        if (esHostList.length == 0) {
            LOG.error("Get host server error.");
            return null;
        }
        boolean isSslEnable = Boolean.parseBoolean(sslEnable);
        if (isSslEnable) {
            return String.format("jdbc:elasticsearch://https://%s", esHostList[0]);
        } else {
            return String.format("jdbc:elasticsearch://http://%s", esHostList[0]);
        }
    }
}
