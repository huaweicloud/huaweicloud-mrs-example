/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.test;

public class NewCounter extends Counter {

    public long getReadValid() {
        return this.read.valid.get();
    }

    public long getReadInvalid() {
        return this.read.invalid.get();
    }

    public long getWriteValid() {
        return this.write.valid.get();
    }

    public long getWriteInvalid() {
        return this.write.invalid.get();
    }

    public long getLatency() {
        return this.latency.valid.get();
    }

    public void addLatency(long latency) {
        this.latency.valid.addAndGet(latency);
    }

    public void addReadValid() {
        this.read.valid.incrementAndGet();
    }

    public void addReadInvalid() {
        this.read.invalid.incrementAndGet();
    }

    public void addWriteValid() {
        this.write.valid.incrementAndGet();

    }

    public void addWriteInvalid() {
        this.write.invalid.incrementAndGet();

    }

    public long getCost2LongValid() {
        return this.cost2long.valid.get();
    }

    public long getCost2LongInValid() {
        return this.cost2long.invalid.get();
    }

    public void addCost2LongValid() {
        this.cost2long.valid.incrementAndGet();
    }

    public void addCost2LongInValid() {
        this.cost2long.invalid.incrementAndGet();
    }

    public Counter getAll() {
        Counter counter = new Counter();
        counter.read.valid.set(getReadValid());
        counter.read.invalid.set(getReadInvalid());
        counter.write.valid.set(getWriteValid());
        counter.write.invalid.set(getWriteInvalid());
        counter.latency.valid.set(getLatency());
        counter.cost2long.invalid.set(getCost2LongInValid());
        counter.cost2long.valid.set(getCost2LongValid());
        return counter;
    }

}
