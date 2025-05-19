/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.javasampleapproach.redis.controller;

import com.javasampleapproach.redis.springdata.RedisClusterKerberosConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RedisKerberosTemplateController {

    @Autowired
    private RedisClusterKerberosConfig kerberosConfig;

    @GetMapping("/kerberos_set")
    public String setKey() {
        kerberosConfig.kerberosRedisTemplate().opsForValue().set("kerberos_key", "kerberos_value");
        return "success";
    }

    @GetMapping("/kerberos_get")
    public Object getKey() {
        return kerberosConfig.kerberosRedisTemplate().opsForValue().get("kerberos_key");
    }
}
