/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.common;

/**
 *
 * @since 2020-09-30
 */
public enum SubmitType {
    /**
     * 命令行方式
     */
    CMD,

    /**
     * 远程方式
     */
    REMOTE,

    /**
     * 本地模式,一般只用来调试
     */
    LOCAL;
}
