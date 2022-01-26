package com.huawei.bigdata.impala.example;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import com.huawei.bigdata.security.LoginUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

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
            init(clientInfo.getClientPrincipal());
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

    private static void init(String principal) throws IOException {
        Configuration conf = new Configuration();

        String userKeytabFile = CONF_DIR + "user.keytab";
        String krb5File = CONF_DIR + "krb5.conf";
        conf.set(HADOOP_SECURITY_AUTHENTICATION, "Kerberos");
        conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");

        LoginUtil.login(principal, userKeytabFile, krb5File, conf);
    }
}

/**
 * Impala client info.
 */
class ClientInfo {
    private String auth = null;
    private String clientPrincipal = null;
    private String serverPrincipal = null;
    private String impalaServer = null;

    private Properties clientInfo = null;

    public ClientInfo(String clientFile) throws IOException {
        clientInfo = new Properties();
        File propertiesFile = new File(clientFile);
        try (InputStream fileInputStream = new FileInputStream(propertiesFile);) {
            clientInfo.load(fileInputStream);
        } catch (Exception e) {
            throw new IOException(e);
        }
        initialize();
    }

    private void initialize() {
        auth = clientInfo.getProperty("auth");
        clientPrincipal = clientInfo.getProperty("client-principal");
        serverPrincipal = clientInfo.getProperty("server-principal");
        impalaServer = clientInfo.getProperty("impala-server");
    }

    public String getAuth() {
        return auth;
    }

    public String getClientPrincipal() {
        return clientPrincipal;
    }

    public String getServerPrincipal() {
        return serverPrincipal;
    }

    public String getImpalaServer() { return impalaServer; }
}
