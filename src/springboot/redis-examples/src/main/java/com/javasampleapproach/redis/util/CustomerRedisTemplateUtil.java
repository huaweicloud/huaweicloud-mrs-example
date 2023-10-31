package com.javasampleapproach.redis.util;

import com.huawei.jredis.client.GlobalConfig;
import com.huawei.jredis.client.SslSocketFactoryUtil;
import com.huawei.jredis.client.auth.AuthConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PostConstruct;
import javax.net.ssl.SSLSocketFactory;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Repository
public class CustomerRedisTemplateUtil {

    protected static final Logger logger = LoggerFactory.getLogger(CustomerRedisTemplateUtil.class.getName());

    private static JedisCluster client;

    /**
     * 默认连接超时时间
     */
    private static final Integer TIMEOUT = 3000;

    /**
     * 最大重试次数
     */
    private static final Integer MAX_ATTEMPTS = 1;

    @Value("${spring.redis.cluster.nodes}")
    private String nodes;

    @Value("${redis.keytab}")
    private String keytab;

    @Value("${redis.user}")
    private String user;

    @Value("${redis.krb5}")
    private String krb5;

    @Value("${redis.realm}")
    private String realm;

    @Value("${redis.ssl}")
    private boolean ssl;

    @PostConstruct
    private void init() throws NoSuchAlgorithmException, KeyManagementException {
        if (Objects.isNull(client)) {

            System.setProperty("redis.authentication.jaas", "true");
            AuthConfiguration authConfig = new AuthConfiguration(krb5, keytab, user);
            GlobalConfig.setAuthConfiguration(authConfig);
            authConfig.setServerRealm(realm);
            authConfig.setLocalRealm(realm);

            logger.info("user={}, realm={}", user, realm);

            // 配置连接池
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(5);
            jedisPoolConfig.setMaxIdle(3);
            jedisPoolConfig.setMinIdle(2);
            Set<HostAndPort> hosts = new HashSet<HostAndPort>();
            for (String node : nodes.split(",")) {
                HostAndPort hp = getIpAndPort(node);
                if (hp == null) {
                    logger.warn("{} not valid", node);
                    continue;
                }
                hosts.add(hp);
            }
            final SSLSocketFactory socketFactory = SslSocketFactoryUtil.createTrustALLSslSocketFactory();
            client = new JedisCluster(hosts, TIMEOUT, TIMEOUT, TIMEOUT, MAX_ATTEMPTS, jedisPoolConfig, ssl,
                socketFactory, authConfig);
        }
    }

    public static HostAndPort getIpAndPort(String hostAndPortStr) {
        if (StringUtils.isBlank(hostAndPortStr)) {
            return null;
        }
        String[] hostAndPort = hostAndPortStr.split(":");
        String ipAddr;
        String port;
        if (hostAndPort.length < 2) {
            return null;
        } else {
            port = hostAndPort[hostAndPort.length - 1];
            ipAddr = hostAndPortStr.substring(0, hostAndPortStr.lastIndexOf(":"));
        }
        return new HostAndPort(ipAddr, Integer.parseInt(port));
    }

    public void save(String key, String value) {
        client.set(key, value);
    }

    public String find(String id) {
        String s = client.get(id);
        System.out.println(s);
        return s;
    }
}
