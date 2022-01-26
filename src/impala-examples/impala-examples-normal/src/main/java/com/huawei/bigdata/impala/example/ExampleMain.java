package com.huawei.bigdata.impala.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class is providing simple example code for using Impala
 */
public class ExampleMain {
    private final static Log LOG = LogFactory.getLog(ExampleMain.class.getName());
    private static final String CONF_DIR = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
    private static final String CLIENT_PROPERTIES = "client.properties";

    public static void main(String[] args) {
        ClientInfo clientInfo;
        try {
            clientInfo = new ClientInfo(CONF_DIR + CLIENT_PROPERTIES);
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }

        JDBCExample jdbcExample = new JDBCExample(clientInfo);
        try {
            jdbcExample.run();
        } catch (Exception e) {
           LOG.error("failed to run jdbcExample, ", e);
        }

    }
}

/**
 * Impala client info.
 * There is only one property when connecting to no-kerberos server.
 * In order to be in keeping with examples of kerberos cluster, reserve ClientInfo class
 */
class ClientInfo {
    private String impalaServer = null;

    private final Properties clientInfo;

    public ClientInfo(String clientFile) throws IOException {
        clientInfo = new Properties();
        File propertiesFile = new File(clientFile);
        try (InputStream fileInputStream = new FileInputStream(propertiesFile)) {
            clientInfo.load(fileInputStream);
        } catch (Exception e) {
            throw new IOException(e);
        }
        initialize();
    }

    private void initialize() {
        impalaServer = clientInfo.getProperty("impala-server");
    }

    public String getImpalaServer() { return impalaServer; }
}
