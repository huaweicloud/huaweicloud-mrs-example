/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.test;

import com.huawei.redis.connect.RedisConnector;
import com.huawei.redis.string.KeyValue;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RedisPerformUnit extends Thread {

    private static final Logger LOG = Logger.getLogger(RedisPerformUnit.class);

    private static final double COST_TOO_LONG_TIME = 100000.0D;

    private RedisConnector rConn = null;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public NewCounter counter = null;

    private int loopNum = 0;

    private boolean loopEnable = false;

    private boolean pipeEnable = false;

    private volatile boolean exit = false;

    private String func = null;

    private double startNum = 0;

    private double stopNum = 0;

    private double keyLen = 0;

    private int valueLen = 0;

    private int fieldLen = 0;

    private int fieldNum = 0;

    private double readRandomX = 0;

    private long sleepTime = 0L;

    private double cost2long;

    public void init(double packets, double startNumIn, double stopNum, int keyLen, int fieldLen, int valueLen, int fieldNum,
        boolean readRandom, boolean loopEnable, boolean pipeEnable, int cost2longIn) {
        this.startNum = startNumIn;
        this.stopNum = stopNum;
        this.keyLen = keyLen;
        this.fieldLen = fieldLen;
        this.valueLen = valueLen;
        this.fieldNum = fieldNum;
        if (readRandom) {
            this.readRandomX = packets;
        } else {
            this.readRandomX = 0;
        }
        this.loopEnable = loopEnable;
        this.pipeEnable = pipeEnable;
        this.cost2long = COST_TOO_LONG_TIME * cost2longIn;
    }

    public RedisPerformUnit(String func) {
        this(func, new RedisConnector());
    }

    public RedisPerformUnit(String func, RedisConnector conn) {
        this.func = func;
        this.rConn = conn;
        this.counter = new NewCounter();
    }

    public void setSleepTime(long time) {
        this.sleepTime = time;
    }

    public void setStop() {
        this.exit = true;
    }

    public void close() {
        if (this.rConn != null) {
            this.rConn.close();
        }
    }

    public int getLoopNum() {
        this.lock.readLock().lock();
        int n = this.loopNum;
        this.lock.readLock().unlock();
        return n;
    }

    public void addLoopNum() {
        this.lock.writeLock().lock();
        this.loopNum += 1;
        this.lock.writeLock().unlock();

        if (!this.loopEnable) {
            setStop();
        }
    }

    public void run() {
        LOG.debug(String.format("start %s from %.0f to %.0f.", this.func, this.startNum, this.stopNum));
        switch (this.func) {
            case "hmset":
                if (this.pipeEnable) {
                    hmsetPipe();
                } else {
                    hmset();
                }
                break;
            case "hget":
                if (this.readRandomX == 0) {
                    hget();
                } else {
                    hgetRandom();
                }
                break;
            case "set":
                if (this.pipeEnable) {
                    setPipe();
                } else {
                    set();
                }
                break;
            case "rpush":
                if (this.pipeEnable) {
                    rpushPipe();
                } else {
                    rpush();
                }
                break;
            case "lrange":
                if (this.pipeEnable) {
                    lrangePipe();
                } else {
                    lrange();
                }
                break;
            case "sadd":
                if (this.pipeEnable) {
                    saddPipe();
                } else {
                    sadd();
                }
                break;
            case "smembers":
                if (this.pipeEnable) {
                    smembersPipe();
                } else {
                    smembers();
                }
                break;
            case "zadd":
                if (this.pipeEnable) {
                    zaddPipe();
                } else {
                    zadd();
                }
                break;
            case "zrange":
                if (this.pipeEnable) {
                    zrangePipe();
                } else {
                    zrange();
                }
                break;
            case "get":
                if (this.readRandomX == 0) {
                    get();
                } else {
                    getRandom();
                }
                break;
        }
    }

    private void doSleep() {
        try {
            if (this.sleepTime != 0L) {
                Thread.sleep(this.sleepTime);
            }
        } catch (InterruptedException e) {
            LOG.info("InterruptedException");
        }
    }

    public void hmset() {
        String rslt = null;
        double i = 0;
        long exceptionTimes = 0L;
        Map<String, String> hashStr = null;
        while (!this.exit) {
            hashStr = KeyValue.getHashValue(this.fieldNum, this.fieldLen, this.valueLen);
            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {
                String key = KeyValue.getKey(this.keyLen, i, false);
                long startTime = System.nanoTime();
                try {
                    rslt = this.rConn.hmset(key, hashStr, false);
                    this.counter.addLatency(System.nanoTime() - startTime);
                    if (rslt.equals("OK")) {
                        this.counter.addWriteValid();
                    } else {
                        this.counter.addWriteInvalid();
                    }
                } catch (Exception e) {
                    this.counter.addLatency(System.nanoTime() - startTime);
                    this.counter.addWriteInvalid();
                    exceptionTimes += 1L;
                    if (exceptionTimes % 500L == 1L) {
                        LOG.debug(
                            String.format("hmset %s exception with %s", KeyValue.getKeyWithSlot(this.keyLen, i, false),
                                e.getMessage()));
                    }
                }
                doSleep();
            }
            addLoopNum();
        }
        if (exceptionTimes > 0L) {
            LOG.debug("hmset error number is " + exceptionTimes);
        }
    }

    public void hmsetPipe() {
        double tmpNum = (this.stopNum - this.startNum) / 1000;
        Map<String, String> hashStr = null;
        List<Object> syncRslt = null;
        while (!this.exit) {
            hashStr = KeyValue.getHashValue(this.fieldNum, this.fieldLen, this.valueLen);
            int i = 0;
            for (double k = this.startNum; (i < tmpNum) && (!this.exit); i++) {
                try {
                    for (int j = 0; (j < 1000) && (!this.exit); k++) {
                        this.rConn.hmset(KeyValue.getKey(this.keyLen, k, false), hashStr, true);
                        j++;
                    }
                    syncRslt = this.rConn.syncAndReturnAll();
                } catch (Exception localException) {
                }
                for (int j = 0; j < syncRslt.size(); j++) {
                    if ((syncRslt.get(j).getClass().equals(String.class)) && (((String) syncRslt.get(j)).equals(
                        "OK"))) {
                        this.counter.addWriteValid();
                    } else {
                        this.counter.addWriteInvalid();
                    }
                }
                doSleep();
            }
            addLoopNum();
        }
    }

    public void hget() {
        String rslt = null;
        double i = 0;
        long exceptionTimes = 0L;
        while (!this.exit) {
            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {

                long startTime = System.nanoTime();
                try {
                    rslt = this.rConn.hget(KeyValue.getKey(this.keyLen, i, false),
                        KeyValue.getHashfield(this.fieldLen, 1, false), false);

                    this.counter.addLatency(System.nanoTime() - startTime);

                    if (rslt.length() > 0) {
                        this.counter.addReadValid();
                    } else {
                        this.counter.addReadInvalid();
                    }
                } catch (Exception e) {
                    this.counter.addLatency(System.nanoTime() - startTime);
                    this.counter.addReadInvalid();
                    exceptionTimes += 1L;
                    if (exceptionTimes % 500L == 1L) {
                        LOG.debug(String.format("hget %s exception(%s) with %s",
                            KeyValue.getKeyWithSlot(this.keyLen, i, false), e.getClass(), e.getMessage()));
                    }
                }
                doSleep();
            }
            addLoopNum();
        }
        if (exceptionTimes > 0L) {
            LOG.debug("hget error number is " + exceptionTimes);
        }
    }

    public void hgetRandom() {
        String rslt = null;
        double i = 0;
        long exceptionTimes = 0L;
        Random random = new Random();
        while (!this.exit) {
            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {
                long startTime = System.nanoTime();
                try {
                    rslt = this.rConn.hget(KeyValue.getKey(this.keyLen, random.nextInt((int)this.readRandomX), false),
                        KeyValue.getHashfield(this.fieldLen, 1, false), false);
                    this.counter.addLatency(System.nanoTime() - startTime);

                    if (rslt == null || rslt.length() > 0) {
                        this.counter.addReadValid();
                    } else {
                        this.counter.addReadInvalid();
                    }
                } catch (Exception e) {
                    this.counter.addLatency(System.nanoTime() - startTime);
                    this.counter.addReadInvalid();
                    exceptionTimes += 1L;
                    if (exceptionTimes % 500L == 1L) {
                        LOG.debug(String.format("hget %s field %s exception(%s) with %s",
                            KeyValue.getKeyWithSlot(this.keyLen, i, false),
                            KeyValue.getHashfield(this.fieldLen, 1, false),
                            e.getClass(), e.getMessage()));
                    }
                }
                doSleep();
            }
            addLoopNum();
        }

        if (exceptionTimes > 0L) {
            LOG.debug("hget error number is " + exceptionTimes);
        }
    }

    public void set() {
        String rslt = null;
        double i = 0;
        long exceptionTimes = 0L;
        while (!this.exit) {
            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {
                String key = KeyValue.getKey(this.keyLen, i, false);
                String value = KeyValue.getString(this.valueLen, true);
                long startTime = System.nanoTime();
                try {
                    rslt = this.rConn.set(key, value, false);
                    long cost = System.nanoTime() - startTime;
                    countCost2Long(cost, true);
                    if (rslt.equals("OK")) {
                        this.counter.addWriteValid();
                    } else {
                        this.counter.addWriteInvalid();
                    }
                } catch (Exception e) {
                    long cost = System.nanoTime() - startTime;
                    countCost2Long(cost, false);
                    this.counter.addWriteInvalid();
                    exceptionTimes += 1L;
                    if (exceptionTimes % 500L == 1L) {
                        LOG.debug(
                            String.format("set %s exception with ", KeyValue.getKeyWithSlot(this.keyLen, i, false)), e);
                    }
                }
                doSleep();
            }
            addLoopNum();
        }
        if (exceptionTimes > 0L) {
            LOG.debug("set error number is " + exceptionTimes);
        }
    }

    private void countCost2Long(long cost, boolean valid) {
        this.counter.addLatency(cost);
        if (cost <= cost2long) {
            return;
        }
        if (valid) {
            this.counter.addCost2LongValid();
            return;
        }
        this.counter.addCost2LongInValid();
    }

    public void setPipe() {
        double tmpNum = (this.stopNum - this.startNum) / 1000;

        List syncRslt = null;
        while (!this.exit) {
            int i = 0;
            for (double k = this.startNum; i < tmpNum; i++) {
                try {
                    for (int j = 0; j < 1000; k++) {
                        this.rConn.set(KeyValue.getKey(this.keyLen, k, false), KeyValue.getString(this.valueLen, true),
                            true);

                        j++;
                    }

                    syncRslt = this.rConn.syncAndReturnAll();
                } catch (Exception localException) {
                }
                for (int j = 0; j < syncRslt.size(); j++) {
                    if ((syncRslt.get(j).getClass().equals(String.class)) && (((String) syncRslt.get(j)).equals(
                        "OK"))) {
                        this.counter.addWriteValid();
                    } else {
                        this.counter.addWriteInvalid();
                    }
                }
                doSleep();
            }
            addLoopNum();
        }
    }

    public void get() {
        String rslt = null;
        double i = 0;
        long exceptionTimes = 0L;
        while (!this.exit) {
            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {
                long startTime = System.nanoTime();
                try {
                    rslt = this.rConn.get(KeyValue.getKey(this.keyLen, i, false), false);
                    this.counter.addLatency(System.nanoTime() - startTime);
                    if (rslt.length() > 0) {
                        this.counter.addReadValid();
                    } else {
                        this.counter.addReadInvalid();
                    }
                } catch (Exception e) {
                    this.counter.addLatency(System.nanoTime() - startTime);
                    this.counter.addReadInvalid();
                    exceptionTimes += 1L;
                    if (exceptionTimes % 500L == 1L) {
                        LOG.debug(String.format("get %s exception: ", KeyValue.getKeyWithSlot(this.keyLen, i, false)),
                            e);
                    }
                }
                doSleep();
            }
            addLoopNum();
        }
        if (exceptionTimes > 0L) {
            LOG.debug("get error number is " + exceptionTimes);
        }
    }

    public void getRandom() {
        String rslt = null;

        long exceptionTimes = 0L;
        Random random = new Random();
        while (!this.exit) {
            for (double i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {
                long startTime = System.nanoTime();
                try {
                    rslt = this.rConn.get(KeyValue.getKey(this.keyLen, random.nextInt((int)this.readRandomX), false), false);

                    this.counter.addLatency(System.nanoTime() - startTime);

                    if (rslt == null || rslt.length() > 0) {
                        this.counter.addReadValid();
                    } else {
                        this.counter.addReadInvalid();
                    }
                } catch (Exception e) {
                    this.counter.addLatency(System.nanoTime() - startTime);
                    this.counter.addReadInvalid();
                    exceptionTimes += 1L;
                    if (exceptionTimes % 500L == 1L) {
                        LOG.debug(String.format("get %s exception with %s",
                                KeyValue.getKeyWithSlot(this.keyLen, i, false), e.getMessage()));
                    }
                }

                doSleep();
            }

            addLoopNum();
        }

        if (exceptionTimes > 0L) {
            LOG.debug("get error number is " + exceptionTimes);
        }
    }

    public void rpush() {
        Long rslt = null;
        double i = 0;
        long exceptionTimes = 0L;
        while (!this.exit) {
            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {
                long startTime = System.nanoTime();
                try {
                    rslt = this.rConn.rpush(KeyValue.getKey(this.keyLen, i, false),
                        KeyValue.getString(this.valueLen, true), false);

                    this.counter.addLatency(System.nanoTime() - startTime);

                    if (rslt.longValue() > 0L) {
                        this.counter.addWriteValid();
                    } else {
                        this.counter.addWriteInvalid();
                    }
                } catch (Exception e) {

                    this.counter.addLatency(System.nanoTime() - startTime);

                    this.counter.addWriteInvalid();

                    exceptionTimes += 1L;

                    if (exceptionTimes % 500L == 1L) {

                        LOG.debug(String.format("rpush %s exception with %s",

                            KeyValue.getKeyWithSlot(this.keyLen, i, false), e.getMessage()));
                    }
                }
                doSleep();
            }
            addLoopNum();
        }
        if (exceptionTimes > 0L) {
            LOG.debug("rpush error number is " + exceptionTimes);
        }
    }

    public void lrange() {
        List<String> rslt = null;
        double i = 0;
        long exceptionTimes = 0L;
        while (!this.exit) {
            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {

                long startTime = System.nanoTime();
                try {
                    rslt = this.rConn.lrange(KeyValue.getKey(this.keyLen, i, false), exceptionTimes, -1L, false);

                    this.counter.addLatency(System.nanoTime() - startTime);

                    if (!rslt.isEmpty()) {
                        this.counter.addReadValid();
                    } else {
                        this.counter.addReadInvalid();
                    }
                } catch (Exception e) {
                    this.counter.addLatency(System.nanoTime() - startTime);
                    this.counter.addReadInvalid();
                    exceptionTimes += 1L;
                    if (exceptionTimes % 500L == 1L) {
                        LOG.debug(String.format("lrange %s exception(%s) with %s",
                            KeyValue.getKeyWithSlot(this.keyLen, i, false), e.getClass(), e.getMessage()));
                    }
                }

                doSleep();
            }
            addLoopNum();
        }
        if (exceptionTimes > 0L) {
            LOG.debug("lrange error number is " + exceptionTimes);
        }
    }

    public void rpushPipe() {
        double tmpNum = (this.stopNum - this.startNum) / 1000;

        List syncRslt = null;

        while (!this.exit) {

            int i = 0;

            for (double k = this.startNum; i < tmpNum; i++) {

                long startTime = System.nanoTime();
                try {

                    for (int j = 0; j < 1000; k++) {

                        this.rConn.rpush(KeyValue.getKey(this.keyLen, k, false),
                            KeyValue.getString(this.valueLen, true), true);

                        j++;
                    }

                    syncRslt = this.rConn.syncAndReturnAll();
                } catch (Exception localException) {
                }

                for (int j = 0; j < syncRslt.size(); j++) {

                    if ((syncRslt.get(j).getClass().equals(String.class)) && (((String) syncRslt.get(j)).equals(
                        "OK"))) {
                        this.counter.addWriteValid();
                    } else {

                        this.counter.addWriteInvalid();
                    }
                }
            }

            addLoopNum();
        }
    }

    public void saddPipe() {

        double tmpNum = (this.stopNum - this.startNum) / 1000;

        List syncRslt = null;

        while (!this.exit) {

            int i = 0;

            for (double k = this.startNum; i < tmpNum; i++) {

                long startTime = System.nanoTime();
                try {

                    for (int j = 0; j < 1000; k++) {

                        this.rConn.sadd(KeyValue.getKey(this.keyLen, k, false), KeyValue.getString(this.valueLen, true),
                            true);

                        j++;
                    }

                    syncRslt = this.rConn.syncAndReturnAll();
                } catch (Exception localException) {
                }

                for (int j = 0; j < syncRslt.size(); j++) {

                    if ((syncRslt.get(j).getClass().equals(String.class)) && (((String) syncRslt.get(j)).equals(
                        "OK"))) {
                        this.counter.addWriteValid();
                    } else {

                        this.counter.addWriteInvalid();
                    }
                }
            }

            addLoopNum();
        }
    }

    public void lrangePipe() {

        List<String> rslt = null;

        double i = 0;

        long exceptionTimes = 0L;

        while (!this.exit) {

            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {

                long startTime = System.nanoTime();
                try {

                    rslt = this.rConn.lrange(KeyValue.getKey(this.keyLen, i, false), exceptionTimes, -1L, false);

                    if (!rslt.isEmpty()) {
                        this.counter.addReadValid();
                    } else {
                        this.counter.addReadInvalid();
                    }
                } catch (Exception e) {

                    this.counter.addReadInvalid();

                    exceptionTimes += 1L;

                    if (exceptionTimes % 500L == 1L) {

                        LOG.debug(
                            String.format("lrange %s exception with %s", KeyValue.getKeyWithSlot(this.keyLen, i, false),
                                e.getMessage()));
                    }
                }
            }

            addLoopNum();
        }

        if (exceptionTimes > 0L) {
            LOG.debug("lrange error number is " + exceptionTimes);
        }
    }

    public void sadd() {

        Long rslt = null;

        double i = 0;

        long exceptionTimes = 0L;

        while (!this.exit) {

            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {

                long startTime = System.nanoTime();
                try {

                    rslt = this.rConn.sadd(KeyValue.getKey(this.keyLen, i, false),
                        KeyValue.getString(this.valueLen, true), false);

                    this.counter.addLatency(System.nanoTime() - startTime);

                    if (rslt > 0L) {
                        this.counter.addWriteValid();
                    } else {
                        this.counter.addWriteInvalid();
                    }
                } catch (Exception e) {

                    this.counter.addLatency(System.nanoTime() - startTime);

                    this.counter.addWriteInvalid();

                    exceptionTimes += 1L;

                    if (exceptionTimes % 500L == 1L) {

                        LOG.debug(
                            String.format("sadd %s exception with %s", KeyValue.getKeyWithSlot(this.keyLen, i, false),
                                e.getMessage()));
                    }
                }

                doSleep();
            }

            addLoopNum();
        }

        if (exceptionTimes > 0L) {
            LOG.debug("sadd error number is " + exceptionTimes);
        }
    }

    public void smembers() {

        Set<String> rslt = null;

        double i = 0;

        long exceptionTimes = 0L;

        while (!this.exit) {

            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {

                long startTime = System.nanoTime();
                try {

                    rslt = this.rConn.smembers(KeyValue.getKey(this.keyLen, i, false), false);

                    this.counter.addLatency(System.nanoTime() - startTime);

                    if (!rslt.isEmpty()) {
                        this.counter.addReadValid();
                    } else {
                        this.counter.addReadInvalid();
                    }
                } catch (Exception e) {

                    this.counter.addLatency(System.nanoTime() - startTime);

                    this.counter.addReadInvalid();

                    exceptionTimes += 1L;

                    if (exceptionTimes % 500L == 1L) {

                        LOG.debug(String.format("smembers %s exception with %s",
                            KeyValue.getKeyWithSlot(this.keyLen, i, false), e.getMessage()));
                    }
                }

                doSleep();
            }

            addLoopNum();
        }

        if (exceptionTimes > 0L) {
            LOG.debug("smembers error number is " + exceptionTimes);
        }
    }

    public void zadd() {

        Long rslt = null;

        double i = 0;

        long exceptionTimes = 0L;

        Map<String, Double> hashStr = null;

        while (!this.exit) {

            hashStr = KeyValue.getHashDoubleValue(this.fieldNum, this.fieldLen, this.valueLen);

            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {

                long startTime = System.nanoTime();
                try {

                    rslt = this.rConn.zadd(KeyValue.getKey(this.keyLen, i, false), hashStr, false);

                    this.counter.addLatency(System.nanoTime() - startTime);

                    if (rslt > -1L) {
                        this.counter.addWriteValid();
                    } else {
                        this.counter.addWriteInvalid();
                    }
                } catch (Exception e) {

                    this.counter.addLatency(System.nanoTime() - startTime);

                    this.counter.addWriteInvalid();

                    exceptionTimes += 1L;

                    if (exceptionTimes % 500L == 1L) {

                        LOG.debug(
                            String.format("zadd %s exception with %s", KeyValue.getKeyWithSlot(this.keyLen, i, false),
                                e.getMessage()));
                    }
                }

                doSleep();
            }

            addLoopNum();
        }

        if (exceptionTimes > 0L) {
            LOG.debug("zadd error number is " + exceptionTimes);
        }
    }

    public void zaddPipe() {

        double tmpNum = (this.stopNum - this.startNum) / 1000;

        Map hashStr = null;

        List syncRslt = null;

        while (!this.exit) {

            hashStr = KeyValue.getHashValue(this.fieldNum, this.fieldLen, this.valueLen);

            int i = 0;

            for (double k = this.startNum; (i < tmpNum) && (!this.exit); i++) {

                long startTime = System.nanoTime();
                try {

                    for (int j = 0; (j < 1000) && (!this.exit); k++) {

                        this.rConn.hmset(KeyValue.getKey(this.keyLen, k, false), hashStr, true);

                        j++;
                    }

                    syncRslt = this.rConn.syncAndReturnAll();
                } catch (Exception localException) {
                }

                for (int j = 0; j < syncRslt.size(); j++) {

                    if ((syncRslt.get(j).getClass().equals(String.class)) && (((String) syncRslt.get(j)).equals(
                        "OK"))) {
                        this.counter.addWriteValid();
                    } else {

                        this.counter.addWriteInvalid();
                    }
                }
            }

            addLoopNum();
        }
    }

    public void zrange() {

        Set<String> rslt = null;

        double i = 0;

        long exceptionTimes = 0L;

        while (!this.exit) {

            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {

                long startTime = System.nanoTime();
                try {

                    rslt = this.rConn.zrange(KeyValue.getKey(this.keyLen, i, false), exceptionTimes, -1L, false);

                    this.counter.addLatency(System.nanoTime() - startTime);

                    if (!rslt.isEmpty()) {
                        this.counter.addReadValid();
                    } else {
                        this.counter.addReadInvalid();
                    }
                } catch (Exception e) {

                    this.counter.addLatency(System.nanoTime() - startTime);

                    this.counter.addReadInvalid();

                    exceptionTimes += 1L;

                    if (exceptionTimes % 500L == 1L) {

                        LOG.debug(String.format("zrange %s exception(%s) with %s",
                            KeyValue.getKeyWithSlot(this.keyLen, i, false), e.getClass(), e.getMessage()));
                    }
                }
                doSleep();
            }
            addLoopNum();
        }
        if (exceptionTimes > 0L) {
            LOG.debug("zrange error number is " + exceptionTimes);
        }
    }

    public void zrangePipe() {

        double tmpNum = (this.stopNum - this.startNum) / 1000;

        Map<String, String> hashStr = null;

        List<Object> syncRslt = null;

        while (!this.exit) {

            hashStr = KeyValue.getHashValue(this.fieldNum, this.fieldLen, this.valueLen);

            int i = 0;

            for (double k = this.startNum; (i < tmpNum) && (!this.exit); i++) {

                long startTime = System.nanoTime();
                try {
                    for (int j = 0; (j < 1000) && (!this.exit); k++) {
                        this.rConn.hmset(KeyValue.getKey(this.keyLen, k, false), hashStr, true);

                        j++;
                    }

                    syncRslt = this.rConn.syncAndReturnAll();
                } catch (Exception localException) {
                }

                for (int j = 0; j < syncRslt.size(); j++) {

                    if ((syncRslt.get(j).getClass().equals(String.class)) && (((String) syncRslt.get(j)).equals(
                        "OK"))) {
                        this.counter.addWriteValid();
                    } else {
                        this.counter.addWriteInvalid();
                    }
                }
            }
            addLoopNum();
        }
    }

    public void smembersPipe() {
        Set<String> rslt = null;
        double i = 0;
        long exceptionTimes = 0L;
        while (!this.exit) {
            for (i = this.startNum; (i < this.stopNum) && (!this.exit); i++) {
                long startTime = System.nanoTime();
                try {
                    rslt = this.rConn.smembers(KeyValue.getKey(this.keyLen, i, false), false);

                    if (!rslt.isEmpty()) {
                        this.counter.addReadValid();
                    } else {
                        this.counter.addReadInvalid();
                    }
                } catch (Exception e) {
                    this.counter.addReadInvalid();
                    exceptionTimes += 1L;
                    if (exceptionTimes % 500L == 1L) {
                        LOG.debug(String.format("smembers %s exception with %s",
                            KeyValue.getKeyWithSlot(this.keyLen, i, false), e.getMessage()));
                    }
                }
            }
            addLoopNum();
        }
        if (exceptionTimes > 0L) {
            LOG.debug("smembers error number is " + exceptionTimes);
        }
    }
}
