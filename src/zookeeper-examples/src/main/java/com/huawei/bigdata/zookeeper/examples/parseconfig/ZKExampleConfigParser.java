package com.huawei.bigdata.zookeeper.examples.parseconfig;

import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class is used to parse the configuration under the path:
 * /conf/configuration.properties.
 */
public class ZKExampleConfigParser
{
    private static final Logger LOG = Logger.getLogger(ZKExampleConfigParser.class.getName());
    private static String confPath = null;
    private static Properties property = new Properties();
    private static String ZOOKEEPER_CONFIG_FILE = "app-config.properties";

    public static void loadConfiguration(String inputConfPath) 
    {
        try
        {
            InputStream inputStream = null;
            if (inputConfPath != null)
            {
                confPath = inputConfPath + File.separator;
                inputStream = new FileInputStream(new File(confPath + ZOOKEEPER_CONFIG_FILE));
            } else {
                confPath = ZKExampleConfigParser.class.getClassLoader().getResource("").getPath();
                inputStream = ZKExampleConfigParser.class.getClassLoader()
                    .getResourceAsStream("app-config.properties");
            }
            property.load(inputStream);
            if (property.isEmpty())
            {
                LOG.info("Error occured when parse configuration file");
            }
        }
        catch (IOException e1)
        {
            LOG.info("Error occured when parse configuration file");
        }
    }

    /**
     * parse value of zookeeper.sasl.client
     * @return value of sasl
     */
    public static String parseSaslClient()
    {
        String saslClient = property.getProperty("saslClient");
        return saslClient;
    }

    /**
     * parse value of user name for kerberos cluster
     * @return user name
     */
    public static String parseUserName()
    {
        String userName = property.getProperty("username");
        return userName;
    }

    /**
     * parse value of zookeeper server principal
     * @return zookeeper server principal
     */
    public static String parseServerPrincipal()
    {
        String serverPrincipal = property.getProperty("serverPrincipal");
        return serverPrincipal;
    }
    
    /**
     * parse path of jass.conf
     * @return path of jass.conf
     */
    public static String parseJaasPath()
    {
        String jaaspath = confPath + property.getProperty("jaasPath");
        return jaaspath;
    }

    /**
     * parse path of keytab
     * @return path of keytab
     */
    public static String parseKeytabPath()
    {
        String keytabPath = confPath + property.getProperty("keytabPath");
        return keytabPath;
    }

    /**
     * parse path of krb5.conf
     * @return path of krb5.conf
     */
    public static String parseKrb5Path()
    {
        String krb5path = confPath + property.getProperty("krb5Path");
        return krb5path;
    }

    /**
     * parse ZooKeeper servers secure connection string
     * @return ZooKeeper servers secure connection string
     */
    public static String parseCxnString()
    {
        String securecxnString = property.getProperty("cxnString");
        return securecxnString;
    }

    /**
     * parse ZooKeeper client session timeout
     * @return Session timeout in milliseconds
     */
    public static String parseTimeout()
    {
        String sessiontimeout = property.getProperty("sessiontimeout");
        return sessiontimeout;
    }
}
