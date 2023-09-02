package com.huawei.bigdata.iotdb;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class IoTDBProperties {
    private final static Logger LOG = LoggerFactory.getLogger(IoTDBProperties.class);

    private static IoTDBProperties instance = null;

    private String iotdb_ssl_enable;
    private String local_host;
    private String local_port;
    private String username;
    private String password;
    private String iotdb_ssl_truststore;

    private IoTDBProperties(){
        System.out.println("current dir: " + System.getProperty("user.dir"));
        String proPath = System.getProperty("user.dir") + File.separator + "iotdb-example.properties";

        try {
            iotdb_ssl_enable = ParameterTool.fromPropertiesFile(proPath).get("iotdb_ssl_enable");
            local_host = ParameterTool.fromPropertiesFile(proPath).get("local_host");
            local_port = ParameterTool.fromPropertiesFile(proPath).get("local_port");
            username = ParameterTool.fromPropertiesFile(proPath).get("username");
            password = ParameterTool.fromPropertiesFile(proPath).get("password");
            iotdb_ssl_truststore = ParameterTool.fromPropertiesFile(proPath).get("iotdb_ssl_truststore");
        } catch (IOException e) {
            LOG.info("The Exception occurred.", e);
        }

    }

    public synchronized static IoTDBProperties getInstance() {
        if (null == instance)
        {
            instance = new IoTDBProperties();
        }

        return instance;
    }

    public String getIotdb_ssl_enable() {
        return iotdb_ssl_enable;
    }

    public String getLocal_host() {
        return local_host;
    }

    public String getLocal_port() {
        return local_port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getIotdb_ssl_truststore() {
        return iotdb_ssl_truststore;
    }
}
