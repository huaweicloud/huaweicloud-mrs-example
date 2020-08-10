package com.huawei.bigdata.hbase.examples;

import com.huawei.hadoop.security.LoginUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestMain {
    private final static Logger LOG = Logger.getLogger(TestMain.class.getName());
    private static String CONF_DIR = null;
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";

    //HBase client configuration which config the rest
    public static String HBASE_CLIENT_PROPERTIES = "hbaseclient.properties";
    private static Configuration conf = null;
    private static String krb5File = null;
    private static String userName = null;
    private static String userKeytabFile = null;
    private static ClientInfo clientInfo = null;
    private static String restServerInfo = null;

    public static void main(String[] args) {
      if (args.length == 1) {
        CONF_DIR = args[0] + File.separator;
        LOG.info("hbase configuration path is :" + CONF_DIR);
      }
        try {
            init();
            login();
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }
        // test hbase normal API
        HBaseExample oneSample;
        try {
            oneSample = new HBaseExample(conf);
            oneSample.test();
        } catch (Exception e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish to test HBase API-------------------");

        // test hbase rest API
        RestExample restTest;
        boolean isEnableSSL = false;
        try {
            if (User.isHBaseSecurityEnabled(conf)) {
                isEnableSSL = true;
            } else {
                isEnableSSL = false;
            }
            restTest = new RestExample(conf, restServerInfo, isEnableSSL);
            restTest.isUseSSL();
        } catch (Exception e) {
            LOG.error("Failed to test HBase REST API because ", e);
        }
        LOG.info("-------finish to test HBase REST API-------");

        // test HIndex API
        HIndexExample hIndexExample;
        try {
            hIndexExample = new HIndexExample(conf);
            hIndexExample.test();
        } catch (Exception e) {
            LOG.error("Failed to test HBase HIndex API because ", e);
        }
        LOG.info("-----finish to test HBase HIndex API-------");
        LOG.info("-----------finish HBase -------------------");
    }

    private static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {

            userName = clientInfo.getUserName();
          if (CONF_DIR == null) {
            ClassLoader classloader = TestMain.class.getClassLoader();
            krb5File = classloader.getResource(clientInfo.getKrb5File()).getPath();
            userKeytabFile = classloader.getResource(clientInfo.getUserKeytabFile()).getPath();
          } else {
            krb5File = CONF_DIR + clientInfo.getKrb5File();
            userKeytabFile = CONF_DIR + clientInfo.getUserKeytabFile();
          }
            System.out.println("userKeytabFile: " + userKeytabFile);
            System.out.println("krb5File: " + krb5File);
          /*
           * if need to connect zk, please provide jaas info about zk. of course,
           * you can do it as below:
           * System.setProperty("java.security.auth.login.config", confDirPath +
           * "jaas.conf"); but the demo can help you more : Note: if this process
           * will connect more than one zk cluster, the demo may be not proper.
           */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    private static void init() throws IOException {
        // load hbase client info
        if(clientInfo == null) {
            clientInfo = new ClientInfo(CONF_DIR, HBASE_CLIENT_PROPERTIES);
            restServerInfo = clientInfo.getRestServerInfo();
        }
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
    }
}
/**
 * hbase client info.
 */
class ClientInfo {
    //The rest server info, format like: ip1:port,ip2:port...
    private String restServerInfo = null;
    private String userName = null;
    private String userKeytabFile = null;
    private String krb5File = null;

    private Properties clientInfo = null;

    public ClientInfo(String confDir, String hbaseclientFile) throws IOException {
        InputStream fileInputStream = null;
        try {
            clientInfo = new Properties();
          if (confDir == null) {
            clientInfo.load(this.getClass().getClassLoader().getResourceAsStream(hbaseclientFile));
          } else {
            clientInfo.load(new FileInputStream(new File(confDir + hbaseclientFile)));
          }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }
        initialize();
    }

    private void initialize() {
        restServerInfo = clientInfo.getProperty("rest.server.info");
        userName = clientInfo.getProperty("user.name");
        userKeytabFile = clientInfo.getProperty("userKeytabName");
        krb5File = clientInfo.getProperty("krb5ConfName");
    }

    public String getRestServerInfo() {
        return restServerInfo;
    }

    public String getUserName() {
        return userName;
    }

    public String getUserKeytabFile() {
        return userKeytabFile;
    }

    public String getKrb5File() {
        return krb5File;
    }
}
