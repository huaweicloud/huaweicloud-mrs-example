package com.huawei.bigdata.iotdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class IoTDBProperties {
    private final static Logger LOG = LoggerFactory.getLogger(IoTDBProperties.class);

    private static Properties iotdbProps = new Properties();

    private static IoTDBProperties instance = null;

    private String iotdb_ssl_enable;
    private String local_host;
    private String local_port;
    private String username;
    private String password;
    private String iotdb_ssl_truststore;
    private String user_keytab_file;
    private String user_principal;
    private String bootstrap_server_url;

    private IoTDBProperties() {
        // If Windows environment, proPath is "iotdb-example.properties" absolute file path.
        // eg:"D:\\sample_project\\sample_project\\src\\iotdb-examples\\iotdb-session-example\\src\\main\\resources\\iotdb-example.properties"
        System.out.println("current dir: " + System.getProperty("user.dir"));
        String proPath = System.getProperty("user.dir") + File.separator + "iotdb-example.properties";
        try {
            iotdbProps.load(new FileInputStream(new File(proPath)));
        } catch (IOException e) {
            LOG.info("The Exception occured.", e);
        }

        iotdb_ssl_enable = iotdbProps.getProperty("iotdb_ssl_enable");
        local_host = iotdbProps.getProperty("local_host");
        local_port = iotdbProps.getProperty("local_port");
        username = iotdbProps.getProperty("username");
        password = iotdbProps.getProperty("password");
        iotdb_ssl_truststore = iotdbProps.getProperty("iotdb_ssl_truststore");
        user_keytab_file = iotdbProps.getProperty("user_keytab_file");
        user_principal = iotdbProps.getProperty("user_principal");
        bootstrap_server_url = iotdbProps.getProperty("bootstrap_server_url");
    }

    public synchronized static IoTDBProperties getInstance() {
        if (null == instance)
        {
            instance = new IoTDBProperties();
        }

        return instance;
    }

    /**
     * get properties value through key
     *
     * @param key properties key
     * @param defValue default value
     * @return
     */
    public String getValues(String key, String defValue) {
        String rtValue = null;

        if (null == key) {
            LOG.error("key is null");
        } else {
            rtValue = getPropertiesValue(key);
        }

        if (null == rtValue) {
            LOG.warn("IoTDBProperties.getValues return null, key is {}", key);
            rtValue = defValue;
        }

        LOG.info("IoTDBProperties.getValues: key is {}; Value is {}", key, rtValue);

        return rtValue;
    }

    private String getPropertiesValue(String key) {
        return iotdbProps.getProperty(key);
    }
}
