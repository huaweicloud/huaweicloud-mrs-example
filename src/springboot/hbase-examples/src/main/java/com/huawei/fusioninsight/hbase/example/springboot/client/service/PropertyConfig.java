/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.hbase.example.springboot.client.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@PropertySource("classpath:springclient.properties") // 指定自定义配置文件位置和名称
@EnableConfigurationProperties(PropertyConfig.class) // 开启对应配置类的属性注入功能
@ConfigurationProperties(prefix = "home") // 指定配置文件注入属性前缀
@Configuration // 自定义配置类
public class PropertyConfig {

    @Value("${principal}")
    private String principal;

    @Value("${user.keytab.path}")
    private String userKeytabFile;

    @Value("${krb5.conf.path}")
    private String krb5File;

    @Value("${conf.path}")
    private String confDir;

    @Value("${zookeeper.server.principal}")
    private String zookeeperPrincipal;

    public String getPrincipal() {
        return principal;
    }

    public String getUserKeytabFile() {
        return userKeytabFile;
    }

    public String getKrb5File() {
        return krb5File;
    }

    public String getConfDir() {
        return confDir;
    }

    public String getZookeeperPrincipal() {
        return zookeeperPrincipal;
    }
}
