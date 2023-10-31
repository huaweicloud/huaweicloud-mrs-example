/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.clickhouse.example.springboot.restclient.controller;

import com.huawei.fusioninsight.clickhouse.example.springboot.restclient.service.ClickHouseExampleService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * clickhouse springboot样例controller
 *
 * @since 2022-12-16
 */
@RestController
@RequestMapping("/clickhouse")
public class ClickHouseExampleController {
    private static final Logger log = LogManager.getLogger(ClickHouseExampleController.class);

    private final ClickHouseExampleService clickHouseExampleService;

    public ClickHouseExampleController(ClickHouseExampleService clickHouseExampleService) {
        this.clickHouseExampleService = clickHouseExampleService;
    }

    /**
     * 执行ClickHouse query
     */
    @GetMapping("/executeQuery")
    public String executeQuery() {
        log.info("Begin to execute query in clickhouse...");
        return clickHouseExampleService.executeQuery();
    }

}
