/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.fusioninsight.hetu.example.springboot.restclient.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ConnectionConfig
{
    @Value("${hetu.host}")
    private String host;

    @Value("${hetu.catalog}")
    private String catalog;

    @Value("${hetu.schema}")
    private String schema;

    @Value("${hetu.user}")
    private String user;

    @Value("${hetu.password}")
    private String password;

    @Value("${hetu.ssl}")
    private String ssl;

    @Value("${hetu.tenant}")
    private String tenant;

    public String getHost()
    {
        return host;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    public String getSsl()
    {
        return ssl;
    }

    public String getTenant()
    {
        return tenant;
    }
}
