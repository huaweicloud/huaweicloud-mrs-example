package com.huawei.bigdata.impala.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import com.huawei.bigdata.security.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

/**
 * This class is providing simple example code for using Impala
 */
public class ExampleMain {
    private final static Log LOG = LogFactory.getLog(ExampleMain.class.getName());
    private static String CONF_DIR = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
    private static final String CLIENT_PROPERTIES = "client.properties";
    private static String userName;
    private static String userKeytabFile;
    private static String krb5File;
    private static Configuration conf;

    public static void main(String[] args) {
        ClientInfo clientInfo;
        boolean isSecurityMode;
        try {
            clientInfo = new ClientInfo(CONF_DIR + CLIENT_PROPERTIES);
            isSecurityMode = "KERBEROS".equalsIgnoreCase(clientInfo.getAuth());
            init(isSecurityMode);
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }

        JDBCExample jdbcExample = new JDBCExample(clientInfo, isSecurityMode);
        try {
            jdbcExample.run();
        } catch (Exception e) {
           LOG.error("failed to run jdbcExample, ", e);
        }

    }

    private static void init(boolean isSecurityMode) throws IOException {
        conf = new Configuration();

        if (isSecurityMode) {
            userName = "impalauser";
            userKeytabFile = CONF_DIR + "user.keytab";
            krb5File = CONF_DIR + "krb5.conf";
            conf.set(HADOOP_SECURITY_AUTHENTICATION, "Kerberos");
            conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");

            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }
}

/**
 * Impala client info.
 */
class ClientInfo {
    private String auth = null;
    private String principal = null;
    private String impalaServer = null;

    private Properties clientInfo = null;

    public ClientInfo(String clientFile) throws IOException {
        InputStream fileInputStream = null;
        try {
            clientInfo = new Properties();
            File propertiesFile = new File(clientFile);
            fileInputStream = new FileInputStream(propertiesFile);
            clientInfo.load(fileInputStream);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
                fileInputStream = null;
            }
        }
        initialize();
    }

    private void initialize() {
        auth = clientInfo.getProperty("auth");
        principal = clientInfo.getProperty("principal");
        impalaServer = clientInfo.getProperty("impala-server");
    }

    public String getAuth() {
        return auth;
    }

    public String getPrincipal() {
        return principal;
    }

    public String getImpalaServer() { return impalaServer; }
}
