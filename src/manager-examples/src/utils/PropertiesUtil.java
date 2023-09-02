package utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * properties 工具类
 */
public class PropertiesUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesUtil.class);

    private static volatile PropertiesUtil instance = null;

    private static volatile Properties config;

    private PropertiesUtil() {
        reloadConfig();
    }

    /**
     * getInstance
     *
     * @return PropertiesUtil
     */
    public static synchronized PropertiesUtil getInstance() {
        if (instance == null) {
            instance = new PropertiesUtil();
        }
        return instance;
    }

    private static void reloadConfig() {
        try (InputStream input = PropertiesUtil.class.getClassLoader().getResourceAsStream("Userinfo.properties")) {
            config = new Properties();
            config.load(input);
        } catch (IOException e) {
            LOG.error("Read properties failed:", e);
        }
    }

    /**
     * getInstance
     *
     * @param key key
     * @return Properties value
     */
    public String getProperties(String key) {
        return config.getProperty(key);
    }

    /**
     * getInstance
     *
     * @param key key
     * @param defaultValue defaultValue
     * @return Properties value
     */
    public String getProperties(String key, String defaultValue) {
        String valueStr = config.getProperty(key);
        return StringUtils.isNotBlank(valueStr) ? valueStr : defaultValue;
    }

    /**
     * getUserName
     *
     * @return user name
     */
    public String getUserName() {
        String userName = config.getProperty("userName");
        LOG.info("The user name is : {}.", userName);
        return userName;
    }

    /**
     * getPassword
     *
     * @return password
     */
    public String password() {
        String password = config.getProperty("password");
        return password;
    }

    /**
     * getWebUrl
     *
     * @return webUrl
     */
    public String getWebUrl() {
        String webUrl = config.getProperty("webUrl");
        LOG.info("The webUrl is : {}.", webUrl);
        return webUrl;
    }
}
