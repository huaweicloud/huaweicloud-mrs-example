/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.fusioninsight.hetu.example.springboot.restclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.huawei.fusioninsight.hetu.example.springboot.restclient.*")
public class HetuApplication
{
    public static void main(String[] args)
    {
        SpringApplication.run(HetuApplication.class, args);
    }
}
