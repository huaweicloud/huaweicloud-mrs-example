/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.springboot.restclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * elasticsearch springboot样例
 *
 * @since 2022-11-14
 */
@SpringBootApplication
@ComponentScan("com.huawei.fusioninsight.elasticsearch.example.springboot.restclient.*")
public class ElasticsearchApplication {
    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchApplication.class, args);
    }
}
