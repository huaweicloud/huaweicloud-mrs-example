/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.clickhouse.example.springboot.restclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 *
 * ClickHouse springboot样例
 *
 * @since 2022-12-16
 */
@SpringBootApplication
@ComponentScan("com.huawei.fusioninsight.clickhouse.example.springboot.restclient.*")
public class ClickHouseApplication {
    public static void main(String[] args) {
        SpringApplication.run(ClickHouseApplication.class, args);
    }
}
