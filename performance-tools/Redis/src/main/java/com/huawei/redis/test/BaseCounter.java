/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.test;

import java.util.concurrent.atomic.AtomicLong;

class BaseCounter {

    public AtomicLong valid = new AtomicLong(0L);

    public AtomicLong invalid = new AtomicLong(0L);

    public void init() {
        this.valid.set(0L);
        this.invalid.set(0L);

    }

    public static BaseCounter add(BaseCounter a, BaseCounter b) {
        BaseCounter c = new BaseCounter();
        c.valid.set(a.valid.get() + b.valid.get());
        c.invalid.set(a.invalid.get() + b.invalid.get());
        return c;

    }

    public static BaseCounter dec(BaseCounter a, BaseCounter b) {
        BaseCounter c = new BaseCounter();
        c.valid.set(a.valid.get() - b.valid.get());
        c.invalid.set(a.invalid.get() - b.invalid.get());
        return c;

    }

    public boolean isZero() {
        return (this.valid.get() == 0L) && (this.invalid.get() == 0L);

    }

}

