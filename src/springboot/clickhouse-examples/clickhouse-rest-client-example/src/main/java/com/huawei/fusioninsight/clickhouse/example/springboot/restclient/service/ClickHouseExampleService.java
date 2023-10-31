/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.clickhouse.example.springboot.restclient.service;

import com.huawei.fusioninsight.clickhouse.example.springboot.restclient.impl.ClickHouseFunc;
import org.springframework.stereotype.Service;

/**
 * clickhouse springboot样例service
 *
 * @since 2022-12-16
 */
@Service
public class ClickHouseExampleService {
    /**
     * 执行clickhouse sql语句
     */
    public String executeQuery() {
        ClickHouseFunc clickHouseFunc = new ClickHouseFunc();
        return clickHouseFunc.executeQuery();
    }

}
