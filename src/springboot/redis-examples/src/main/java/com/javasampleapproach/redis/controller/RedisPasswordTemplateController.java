/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.javasampleapproach.redis.controller;

import com.javasampleapproach.redis.springdata.RedisClusterPasswordConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RedisPasswordTemplateController {
    @Autowired
    private RedisClusterPasswordConfig redisClusterConfig;

    @GetMapping("/set")
    public String setKey() {
        redisClusterConfig.redisTemplate().opsForValue().set("password_key", "password_value");
        return "success";
    }

    @GetMapping("/get")
    public Object getKey() {
        return redisClusterConfig.redisTemplate().opsForValue().get("password_key");
    }
}
