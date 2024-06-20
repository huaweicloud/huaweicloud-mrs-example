/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.fusioninsight.hetu.example.springboot.restclient.controller;

import com.huawei.fusioninsight.hetu.example.springboot.restclient.service.HetuExampleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/hetu/example")
public class HetuExampleController
{
    @Autowired
    private HetuExampleService hetuExampleService;

    @GetMapping("/executesql")
    public String executeSql()
    {
        return hetuExampleService.executeSql();
    }
}
