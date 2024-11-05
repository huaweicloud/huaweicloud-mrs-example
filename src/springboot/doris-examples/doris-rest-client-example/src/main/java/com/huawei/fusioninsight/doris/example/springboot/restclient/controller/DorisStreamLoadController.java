/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.doris.example.springboot.restclient.controller;


import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.huawei.fusioninsight.doris.example.springboot.restclient.service.DorisStreamLoaderService;

/**
 * doris springboot样例controller
 *
 * @since 2022-11-14
 */
@RestController
@RequestMapping("/doris/example/streamload")
public class DorisStreamLoadController {

    @Autowired
    private DorisStreamLoaderService dorisExampleService;

    /**
     * 执行doris sql
     */
    @GetMapping("/executesql")
    public String executeSql() throws IOException {
        dorisExampleService.initTable();
        String path = DorisStreamLoadController.class.getClassLoader().getResource("test.csv").getPath();
        path = URLDecoder.decode(path, "UTF-8");
        File file = new File(path);
        String filePath = file.getAbsolutePath();
        // 在linux场景需要预先将resource目录下test.csv文件上传到linux后台，然后在getHttpPost中替换对应的文件路径，如/root/test.csv
        return dorisExampleService.getHttpPost(filePath);
    }
}
