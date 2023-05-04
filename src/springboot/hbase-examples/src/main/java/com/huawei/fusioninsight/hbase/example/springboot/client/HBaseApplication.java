/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.hbase.example.springboot.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * hbase springboot样例
 *
 * @since 2022
 */
@SpringBootApplication
@ComponentScan("com.huawei.fusioninsight.hbase.example.springboot.client.*")
public class HBaseApplication {
    public static void main(String[] args) {
        SpringApplication.run(HBaseApplication.class, args);
    }
}
