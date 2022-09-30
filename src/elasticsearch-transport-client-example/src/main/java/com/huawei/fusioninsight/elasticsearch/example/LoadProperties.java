/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example;

import com.huawei.fusioninsight.elasticsearch.transport.common.Configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * 加载transport客户端配置文件
 *
 * @since 2020-03-30
 */
public class LoadProperties {
    private static final Logger LOG = LogManager.getLogger(LoadProperties.class);

    private static Properties properties = new Properties();

    private static final String CONFIGURATION_FILE_NAME = "esParams.properties";

    /**
     * 配置文件默认配置路径
     */
    private static final String DEFAULT_CONFIG_PATH = "conf";

    private static final String JAR_EXT = ".jar";

    private static final String KEY_TAB_PATH = "keytabPath";

    private static final String KRB_5_PATH = "krb5Path";

    private static final String CUSTOM_JAAS_PATH = "customJaasPath";

    private static String configPath;

    /**
     * 加载配置文件
     *
     * @param args 参数列表
     * @return 配置对下
     * @throws IOException 异常
     */
    public static Configuration loadProperties(String[] args) throws IOException {
        initConfigPath(args);
        initProperties();
        Configuration configuration = new Configuration();
        configuration.setClusterName(loadClusterName());
        configuration.setTransportAddress(loadTransportAddress());
        configuration.setSecureMode(loadIsSecureMode());
        if (configuration.isSecureMode()) {
            configuration.setPrincipal(loadPrincipal());
            configuration.setKeyTabPath(loadPath(KEY_TAB_PATH));
            configuration.setKrb5Path(loadPath(KRB_5_PATH));
            configuration.setSslEnabled(loadSslEnabled());
            configuration.setCustomJaasPath(loadCustomJaasPath());
        }
        configuration.setSniff(loadIsSniff());
        LOG.info("configuration: {}", configuration);
        return configuration;
    }

    private static void initProperties() {
        try {
            properties.load(new FileInputStream(new File(configPath)));
        } catch (IOException e) {
            LOG.error("Failed to load properties file, config file path is {}.", configPath, e);
            throw new IllegalArgumentException();
        }
    }

    private static void initConfigPath(String[] args) throws IOException {
        // 配置文件路径参数位置
        int configPathArgumentIndex = 0;
        if (args == null || args.length < 1 || args[configPathArgumentIndex] == null
            || args[configPathArgumentIndex].isEmpty()) {
            configPath = getCurrentJarPath() + CONFIGURATION_FILE_NAME;
        } else {
            configPath = args[configPathArgumentIndex];
            File configFile = new File(configPath);
            if (configFile.exists() && configFile.isDirectory()) {
                configPath = configPath + File.separator + CONFIGURATION_FILE_NAME;
            }
        }
    }

    /**
     * 读取clusterName参数
     *
     * @return 参数值
     */
    public static String loadClusterName() {
        String clusterName = properties.getProperty("clusterName");
        if (clusterName == null || clusterName.isEmpty()) {
            LOG.error("clusterName is empty, please configure it in config file : {}.", configPath);
            throw new IllegalArgumentException();
        }
        return clusterName;
    }

    private static Set<TransportAddress> loadTransportAddress() {
        String serverHosts = properties.getProperty("esServerHosts");
        if (serverHosts == null || serverHosts.isEmpty()) {
            LOG.error("Please configure esServerHosts in conf/{}.", CONFIGURATION_FILE_NAME);
            LOG.error("The format of esServerHosts is ip1:port1,ip2:port2,ipn:portn");
            throw new IllegalArgumentException();
        }
        String[] hosts = serverHosts.split(",");
        Set<TransportAddress> transportAddresses = new HashSet<>(hosts.length);
        for (String host : hosts) {
            String esClientIp = "";
            int port = -1;
            final int portIdx = host.lastIndexOf(":");
            if (portIdx > 0) {
                try {
                    port = Integer.parseInt(host.substring(portIdx + 1));
                } catch (final NumberFormatException ex) {
                    throw new IllegalArgumentException("Invalid HTTP host: " + host);
                }
                esClientIp = host.substring(0, portIdx);
            }
            if (-1 == port) {
                LOG.error("The configuration  esClientIPPort is empty, please configure it in config file : {}.",
                    configPath);
                throw new IllegalArgumentException();
            }
            try {
                if (port % 2 == 0) {
                    LOG.warn("The configuration esClientIPPort may be wrong, please check it in config file : {}.",
                        configPath);
                }
            } catch (NumberFormatException e) {
                LOG.warn("The configuration esClientIPPort may be wrong, please check it in config file : {}.",
                    configPath);
                throw new IllegalArgumentException();
            }
            try {
                transportAddresses.add(new TransportAddress(InetAddress.getByName(esClientIp), port));
            } catch (NumberFormatException | UnknownHostException e) {
                LOG.error("Init esServerHosts occur error : {}.", e.getMessage());
                throw new IllegalArgumentException();
            }
        }
        return transportAddresses;
    }

    private static String loadPath(String path) throws IOException {
        String loadedPath = properties.getProperty(path);
        if (loadedPath == null || loadedPath.isEmpty()) {
            loadedPath = getCurrentJarPath();
            if (!loadedPath.endsWith(File.separator)) {
                loadedPath += File.separator;
            }
            LOG.warn("Config path is empty, using the default path : {} .", loadedPath);
        }
        return loadedPath;
    }

    private static String loadCustomJaasPath() {
        String jaasPath = properties.getProperty(CUSTOM_JAAS_PATH);
        if (jaasPath == null || jaasPath.isEmpty()) {
            return "";
        }

        return jaasPath;
    }

    private static boolean loadIsSecureMode() {
        return !Boolean.FALSE.toString().equals(properties.getProperty("isSecureMode"));
    }

    private static boolean loadSslEnabled() {
        return !Boolean.FALSE.toString().equals(properties.getProperty("sslEnabled"));
    }

    private static boolean loadIsSniff() {
        return !Boolean.FALSE.toString().equals(properties.getProperty("isSniff"));
    }

    private static String loadPrincipal() {
        String principal = properties.getProperty("principal");
        if (principal == null || principal.isEmpty()) {
            LOG.error("Please configure principal config file : {} .", configPath);
            throw new IllegalArgumentException();
        }
        return principal;
    }

    private static String getCurrentJarPath() throws IOException {
        StringBuilder currentJarPath = new StringBuilder();
        URL url = LoadProperties.class.getProtectionDomain().getCodeSource().getLocation();
        try {
            String filePath = URLDecoder.decode(url.getPath(), "UTF-8");
            if (filePath.endsWith(JAR_EXT)) {
                // 获取jar包所在目录
                filePath = filePath.substring(0, filePath.lastIndexOf(File.separator) + 1);
                File file = new File(filePath);
                currentJarPath.append(file.getCanonicalPath());
                if (!currentJarPath.toString().endsWith(File.separator)) {
                    currentJarPath.append(File.separator);
                }
                currentJarPath.append("..").append(File.separator).append(DEFAULT_CONFIG_PATH).append(File.separator);
            } else {
                currentJarPath.append(System.getProperty("user.dir"))
                    .append(File.separator)
                    .append(DEFAULT_CONFIG_PATH)
                    .append(File.separator);
            }
        } catch (IOException ex) {
            LOG.error("Get current jar path error.", ex);
            throw ex;
        }
        return currentJarPath.toString();
    }
}
