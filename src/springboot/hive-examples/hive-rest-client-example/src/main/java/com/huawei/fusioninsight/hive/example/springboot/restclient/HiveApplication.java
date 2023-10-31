/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.hive.example.springboot.restclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Hive springboot样例
 *
 * @since 2022-12-02
 */
@SpringBootApplication
@ComponentScan("com.huawei.fusioninsight.hive.example.springboot.restclient.*")
public class HiveApplication {
    public static void main(String[] args) {
        SpringApplication.run(HiveApplication.class, args);
    }
}
