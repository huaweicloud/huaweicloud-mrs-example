/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.doris.example.springboot.restclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Doris springboot样例
 *
 * @since 2022-12-02
 */
@SpringBootApplication
@ComponentScan("com.huawei.fusioninsight.doris.example.springboot.restclient.*")
public class DorisApplication {
    public static void main(String[] args) {
        SpringApplication.run(DorisApplication.class, args);
    }
}
