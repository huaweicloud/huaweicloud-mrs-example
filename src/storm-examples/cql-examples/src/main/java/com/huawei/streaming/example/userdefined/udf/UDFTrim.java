/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.streaming.example.userdefined.udf;

import com.huawei.streaming.udfs.UDF;

import java.util.Map;

/**
 * 移除字符串前后特殊字符
 *
 * @since 2020-08-22
 */
public class UDFTrim extends UDF {
    private static final long serialVersionUID = 4793756788804334850L;

    /**
     * <默认构造函数>
     *
     * @param config udf函数中需要的参数，
     * 这些参数要在cql中通过create function xx properties语法进行设置
     */
    public UDFTrim(Map<String, String> config) {
        super(config);
    }

    /**
     * UDF函数的执行方法
     * 方法名称必须是evaluate。
     *
     * @param s 字符串
     * @return 移除空格之后的字符串
     * @see [类、类#方法、类#成员]
     */
    public String evaluate(String str) {
        if (str == null) {
            return null;
        }

        return str.trim();
    }
}
