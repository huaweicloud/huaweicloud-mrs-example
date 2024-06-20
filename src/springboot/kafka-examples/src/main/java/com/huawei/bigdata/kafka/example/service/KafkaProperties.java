/**
 * Copyright Notice:
 *      Copyright  2013-2024, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */

package com.huawei.bigdata.kafka.example.service;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaProperties {

    // Common Client Config
    @Value("${bootstrap.servers:}")
    private String bootstrapServers;

    @Value("${security.protocol:SASL_PLAINTEXT}")
    private String securityProtocol;

    @Value("${sasl.mechanism:PLAIN}")
    private String saslMechanism;

    @Value("${manager_username:}")
    private String username;

    @Value("${manager_password:}")
    private String password;

    @Value("${topic:example-metric1}")
    private String topic;

    @Value("${is.security.mode:true}")
    private boolean isSecurityMode;

    // producer config
    @Value("${isAsync:false}")
    private String isAsync;

    // consumer config
    @Value("${consumer.alive.time:180000}")
    private String consumerAliveTime;

    public KafkaProperties() {
    }

    public void initialClientProperties(Properties properties) {
        // Broker连接地址
        if (isEmpty(this.bootstrapServers)) {
            throw new IllegalArgumentException("The bootstrap.servers is null or empty.");
        }
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);

        // 安全协议类型
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, this.securityProtocol);
        // 安全协议下使用的认证机制
        properties.setProperty(SaslConfigs.SASL_MECHANISM, this.saslMechanism);

        // 动态jaas config
        if (this.isSecurityMode) {
            if (isEmpty(this.username)|| isEmpty(this.password)) {
                throw new IllegalArgumentException("The properties manager_username or manager_password is null or empty.");
            }

            String jaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=%s password=%s;", this.username, this.password);
            properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        }

        properties.setProperty("topic", this.topic);
        properties.setProperty("isAsync", this.isAsync);
        properties.setProperty("consumer.alive.time", this.consumerAliveTime);
    }

    public static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }
}
