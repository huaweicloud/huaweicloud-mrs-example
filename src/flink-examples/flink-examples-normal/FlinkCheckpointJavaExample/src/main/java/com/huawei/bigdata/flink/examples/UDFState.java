/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import java.io.Serializable;

/**
 * 自定义状态类
 * 
 * @since 2019/9/30
 */
public class UDFState implements Serializable {
    private long count;

    public UDFState() {
        count = 0L;
    }

    /**
     * 设置数量
     * 
     * @param count 数量
     */
    public void setState(long count) {
        this.count = count;
    }

    /**
     * @return 数量
     */
    public long getState() {
        return this.count;
    }
}
