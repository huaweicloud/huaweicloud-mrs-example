/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import java.io.Serializable;

/**
 * @since 8.0.2
 */
public class UDFState implements Serializable {
    private long count;

    /**
     * @since 8.0.2
     */
    public UDFState() {
        count = 0L;
    }

    /**
     * @param count param1
     */
    public void setState(long count) {
        this.count = count;
    }

    /**
     * @return status
     */
    public long getState() {
        return this.count;
    }
}
