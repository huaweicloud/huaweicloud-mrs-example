
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.test;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AsyncSleep extends Thread {

     boolean isContinue = true;

     int interval = 5000;

     ReadWriteLock lock = new ReentrantReadWriteLock();

    public AsyncSleep(int interval) {
        this.interval = interval;

    }

    public void set(boolean isCon) {
        this.lock.writeLock().lock();
        this.isContinue = isCon;
        this.lock.writeLock().unlock();

    }

    public void run() {

        while (true) {
            this.lock.readLock().lock();
            if (!this.isContinue) {
                break;
            }

            try {
                Thread.sleep(this.interval);

            } catch (Exception e) {
                e.printStackTrace();

            }
            this.lock.readLock().unlock();

        }
        this.lock.readLock().unlock();

    }

}

