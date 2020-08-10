package com.huawei.bigdata.hive.example;

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
 * This class is providing simple example code for using hive
 */
public class ExampleMain {
    private final static Log LOG = LogFactory.getLog(ExampleMain.class.getName());
    private static String CONF_DIR = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";
    //Hive client configuration, it is located in $HIVE_CLIENT/config
    public static final String HIVE_CLIENT_PROPERTIES = "hiveclient.properties";
    private static String userName;
    private static String userKeytabFile;
    private static String krb5File;
    private static Configuration conf;

    public static void main(String[] args) {
        ClientInfo clientInfo;
        boolean isSecurityMode;
        try {
            clientInfo = new ClientInfo(CONF_DIR + HIVE_CLIENT_PROPERTIES);
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
        /**
         * Other way to set conf for zk. If use this way,
         * can ignore the way in the 'login' method
         */
        if (isSecurityMode) {
            userName = "hiveuser";
            userKeytabFile = CONF_DIR + "user.keytab";
            krb5File = CONF_DIR + "krb5.conf";
            conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");

            /**
             * One way for connect zk, Note: if this process
             * will connect more than one zk cluster, this way may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);

            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }
}

/**
 * Hive client info.
 */
class ClientInfo {
    //The zk quorum info, format like: ip1:port,ip2:port...
    private String zkQuorum = null;
    private String auth = null;
    private String saslQop = null;
    private String zooKeeperNamespace = null;
    private String serviceDiscoveryMode = null;
    private String principal = null;

    private Properties clientInfo = null;

    public ClientInfo(String hiveclientFile) throws IOException {
        InputStream fileInputStream = null;
        try {
            clientInfo = new Properties();
            File propertiesFile = new File(hiveclientFile);
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
        zkQuorum = clientInfo.getProperty("zk.quorum");
        auth = clientInfo.getProperty("auth");
        saslQop = clientInfo.getProperty("sasl.qop");
        zooKeeperNamespace = clientInfo.getProperty("zooKeeperNamespace");
        serviceDiscoveryMode = clientInfo.getProperty("serviceDiscoveryMode");
        principal = clientInfo.getProperty("principal");
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public String getSaslQop() {
        return saslQop;
    }

    public String getAuth() {
        return auth;
    }

    public String getZooKeeperNamespace() {
        return zooKeeperNamespace;
    }

    public String getServiceDiscoveryMode() {
        return serviceDiscoveryMode;
    }

    public String getPrincipal() {
        return principal;
    }
}
