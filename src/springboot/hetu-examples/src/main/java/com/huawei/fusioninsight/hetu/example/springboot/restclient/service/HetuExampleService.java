/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.fusioninsight.hetu.example.springboot.restclient.service;

import com.huawei.fusioninsight.hetu.example.springboot.restclient.util.HetuDataSourceUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;

@Service
public class HetuExampleService
{
    private static final Logger logger = LogManager.getLogger(HetuExampleService.class);

    @Autowired
    private ConnectionConfig connectionConfig;

    public String executeSql()
    {
        String querySql = "SHOW TABLES";

        StringBuilder result = new StringBuilder();
        String separator = System.getProperty("line.separator");
        result.append("=========================== Hetu Example Start ===========================");

        try (Connection connection = HetuDataSourceUtil.createConnection(connectionConfig)) {
            // ad-hoc query
            result.append(separator).append("Start to query table data.");
            String queryResult = HetuDataSourceUtil.executeQuery(connection, querySql);
            result.append(separator).append("Query result : ").append(separator).append(queryResult);
            result.append(separator).append("Querying table data succeeded.");

            result.append(separator).append("=========================== Hetu Example End ===========================");
        }
        catch (Exception e) {
            logger.error("Hetu springboot example execution failure, detail exception : " + e);
            return "Hetu springboot example execution failure, detail exception : " + e;
        }

        logger.info("Hetu springboot example execution successfully.");
        return result.toString();
    }
}
