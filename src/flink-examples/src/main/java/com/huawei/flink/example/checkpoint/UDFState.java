package com.huawei.flink.example.checkpoint;

import java.io.Serializable;

public class UDFState implements Serializable {

    private long count;

    public UDFState()
    {
        count = 0;
    }

    public long getCount()
    {
        return count;
    }

    public void setCount(long count)
    {
        this.count = count;
    }
}

