/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 */

package com.huawei.fusioninsight.doris.example.springboot.restclient.controller;


import com.huawei.fusioninsight.doris.example.springboot.restclient.service.DBalancerExampleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * doris springboot sample example controller
 *
 * @since 2024-02-22
 */
@RestController
@RequestMapping("/doris/example/dbalancer")
public class DBalancerExampleController {

    @Autowired
    private DBalancerExampleService dBalancerExampleService;

    /**
     * 执行doris sql
     */
    @GetMapping("/executesql")
    public String executeSql() {
        return dBalancerExampleService.executeSql();
    }

}
