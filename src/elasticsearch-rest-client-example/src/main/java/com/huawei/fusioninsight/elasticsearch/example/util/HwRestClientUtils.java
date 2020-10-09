/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.util;

import org.elasticsearch.hwclient.HwRestClient;

import java.io.File;
import java.io.IOException;

/**
 * 客户端工具
 *
 * @since 2020-09-30
 */
public class HwRestClientUtils {
    /**
     * 配置文件路径位置
     */
    private static final int CONFIG_PATH_ARGUMENT_INDEX = 0;

    /**
     * 获取HwRestClient
     *
     * @param args 配置参数
     * @return HwRestClient
     */
    public static HwRestClient getHwRestClient(String[] args) {
        HwRestClient hwRestClient;
        if (args == null
                || args.length < 1
                || args[CONFIG_PATH_ARGUMENT_INDEX] == null
                || args[CONFIG_PATH_ARGUMENT_INDEX].isEmpty()) {
            hwRestClient = new HwRestClient();
        } else {
            String configPath = args[CONFIG_PATH_ARGUMENT_INDEX];
            File configFile = new File(configPath);
            if (configFile.exists()) {
                if (configFile.isDirectory()) {
                    hwRestClient = new HwRestClient(configPath);
                } else {
                    try {
                        hwRestClient =
                                new HwRestClient(
                                        configFile
                                                .getCanonicalPath()
                                                .substring(
                                                        0,
                                                        configFile.getCanonicalPath().lastIndexOf(File.separator) + 1),
                                        configFile.getName());
                    } catch (IOException e) {
                        hwRestClient = new HwRestClient();
                    }
                }
            } else {
                hwRestClient = new HwRestClient();
            }
        }
        return hwRestClient;
    }
}
