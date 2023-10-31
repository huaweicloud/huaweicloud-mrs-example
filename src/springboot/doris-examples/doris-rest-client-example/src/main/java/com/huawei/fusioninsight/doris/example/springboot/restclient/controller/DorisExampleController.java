/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.doris.example.springboot.restclient.controller;


import com.huawei.fusioninsight.doris.example.springboot.restclient.service.DorisExampleService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * doris springboot样例controller
 *
 * @since 2022-11-14
 */
@RestController
@RequestMapping("/doris/example")
public class DorisExampleController {

    @Autowired
    private DorisExampleService dorisExampleService;

    /**
     * 执行doris sql
     */
    @GetMapping("/executesql")
    public String executeSql() {
        return dorisExampleService.executeSql();
    }

}
