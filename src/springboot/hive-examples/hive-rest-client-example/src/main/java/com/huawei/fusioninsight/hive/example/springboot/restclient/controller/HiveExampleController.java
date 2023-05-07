/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.hive.example.springboot.restclient.controller;


import com.huawei.fusioninsight.hive.example.springboot.restclient.service.HiveExampleService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Hive springboot样例controller
 *
 * @since 2022-11-14
 */
@RestController
@RequestMapping("/hive/example")
public class HiveExampleController {

    @Autowired
    private HiveExampleService hiveExampleService;

    /**
     * 执行hive sql
     */
    @GetMapping("/executesql")
    public String executeSql() {
        return hiveExampleService.executesql();
    }

}
