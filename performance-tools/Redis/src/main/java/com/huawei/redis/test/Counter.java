/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.test;

public class Counter {

    public static final int ROW_NUM = 6;

    protected BaseCounter read = new BaseCounter();

    protected BaseCounter write = new BaseCounter();

    protected BaseCounter latency = new BaseCounter();

    protected BaseCounter cost2long = new BaseCounter();

    public void init() {
        this.read.init();
        this.write.init();
        this.latency.init();
        this.cost2long.init();
    }

    public static Counter add(Counter a, Counter b) {
        Counter c = new Counter();
        c.read = BaseCounter.add(a.read, b.read);
        c.write = BaseCounter.add(a.write, b.write);
        c.latency = BaseCounter.add(a.latency, b.latency);
        c.cost2long = BaseCounter.add(a.cost2long, b.cost2long);
        return c;

    }

    public static Counter dec(Counter a, Counter b) {
        Counter c = new Counter();
        c.read = BaseCounter.dec(a.read, b.read);
        c.write = BaseCounter.dec(a.write, b.write);
        c.latency = BaseCounter.dec(a.latency, b.latency);
        c.cost2long = BaseCounter.dec(a.cost2long, b.cost2long);
        return c;

    }

    public boolean isZero() {
        return (this.read.isZero()) && (this.write.isZero()) && (this.latency.isZero());

    }

    public double[] getTps(long t) {
        double[] tps = new double[ROW_NUM];
        tps[0] = this.read.valid.get() * 1000.0D / t;
        tps[1] = this.read.invalid.get() * 1000.0D / t;
        tps[2] = this.write.valid.get() * 1000.0D / t;
        tps[3] = this.write.invalid.get() * 1000.0D / t;
        long totalCount = this.read.valid.get() + this.read.invalid.get() + this.write.valid.get()
            + this.write.invalid.get();
        tps[4] = totalCount == 0L ? 0.0D : this.latency.valid.get() / totalCount / 100000.0D;
        tps[5] = this.cost2long.valid.get() + this.cost2long.invalid.get();
        return tps;

    }

}

