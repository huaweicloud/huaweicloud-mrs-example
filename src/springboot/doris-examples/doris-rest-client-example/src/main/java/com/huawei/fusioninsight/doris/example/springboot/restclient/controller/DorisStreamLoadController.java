/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.doris.example.springboot.restclient.controller;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.Properties;

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
        Properties properties = new Properties();
        InputStream in = DorisStreamLoadController.class.getClassLoader().getResourceAsStream("conf.properties");
        if (in == null) {
            return "conf.properties is not exist";
        }
        properties.load(in);
        if (properties.getProperty("CSV_FILE_PATH") != null) {
            filePath = properties.getProperty("CSV_FILE_PATH");
        }
        return dorisExampleService.getHttpPost(filePath);
    }
}
