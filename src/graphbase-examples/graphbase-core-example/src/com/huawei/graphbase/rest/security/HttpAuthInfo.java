package com.huawei.graphbase.rest.security;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;

public class HttpAuthInfo {
    private static final String HTTP_URL_PREFIX = "https://";

    private static final String RESOUCE_SEP = "/";

    private static final Range<Integer> PORT_RANGE = Range.between(0, 65536);

    /**
     * IP
     */
    private final String ip;

    /**
     * service port
     */
    private final int port;

    /**
     * service name
     */
    private final String service;

    /**
     * user name
     */
    private final String username;

    /**
     * password
     */
    private final String password;

    /**
     * keytabfile
     */
    private final String keytabFilePath;

    /**
     * base url
     */
    private final String baseUrl;

    /**
     * <p> 构造器 </p>
     */
    HttpAuthInfo(Builder builder) {
        this.ip = builder.ip;
        this.port = builder.port;
        this.service = builder.service;
        this.username = builder.username;
        this.password = builder.password;
        this.keytabFilePath = builder.keytabFilePath;
        this.baseUrl = HTTP_URL_PREFIX + ip + ":" + port + RESOUCE_SEP + service;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getService() {
        return service;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getKeytabFilePath() {
        return keytabFilePath;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    @Override
    public String toString() {
        return "BASE URL:" + baseUrl + "\n" + "LOGIN IN:[" + username + "]";
    }

    public static class Builder {
        public String password;

        public String keytabFilePath;

        private String ip;

        private int port = -1;

        private String service;

        private String username;

        Builder() {
        }

        public Builder setIp(String ip) {
            this.ip = ip;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setService(String service) {
            this.service = service;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setKeytabFile(String keytabFile) {
            this.keytabFilePath = keytabFile;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public HttpAuthInfo build() {
            StringUtils.isEmpty("");
            if (StringUtils.isEmpty(ip)) {
                throw new IllegalArgumentException("invalid ip:" + ip);
            }
            if (port <= PORT_RANGE.getMinimum() || port >= PORT_RANGE.getMaximum()) {
                throw new IllegalArgumentException("invalid port:" + port);
            }
            if (StringUtils.isEmpty(username)) {
                throw new IllegalArgumentException("invalid username:" + username);
            }
            if (StringUtils.isEmpty(password) && StringUtils.isEmpty(keytabFilePath)) {
                throw new IllegalArgumentException("both password and keytabFile can not be null");
            }
            return new HttpAuthInfo(this);
        }
    }

}
