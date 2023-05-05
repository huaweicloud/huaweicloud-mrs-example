/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.test;

import static com.huawei.redis.test.Counter.ROW_NUM;

import com.huawei.redis.connect.RedisConnector;
import com.huawei.redis.string.KeyValue;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class RedisPerform {

    private static final Logger LOG = Logger.getLogger(RedisPerform.class);

    private static final String CFG_FILE = "test.properties";

    private static final String LOG_PATH = "/var/log/RedisPerform";

    private static final String TEST_TIME = "redis.test.time";

    private static final String READ_TYPE = "redis.read.random";

    private static final String TEST_PIPELINE = "redis.test.pipeline";

    private static final String TEST_KEY_LEN = "redis.test.key.len";

    private static final String TEST_VALUE_LEN = "redis.test.value.len";

    private static final String TEST_FIELD_LEN = "redis.test.field.len";

    private static final String TEST_FIELD_NUM = "redis.test.field.number";

    private static final String TEST_SLEEP_MS = "sleep.per.op";

    private static final String VERSION = "1.1.0";

    RedisConnector rConn = null;

    List<Counter> tps = new LinkedList<>();

    private int testTime = 0;

    private int interval = 5;

    private int readRandom = 0;

    private int pipeEnable = 0;

    private int threadNum = 1;

    private String func = null;

    private double startNum = 0;

    private double stopNum = 0;

    private int keyLen = 0;

    private int fieldNum = 0;

    private int fieldLen = 0;

    private int valueLen = 0;

    private long sleepTime = 0L;

    private int cost2long = 300;


    List<RedisPerformUnit> threadList = new ArrayList<>();

    private void initClientCfg() {
        File dir = new File(LOG_PATH);
        if (!dir.exists()) {
            dir.mkdir();
        }

        Properties properties = new Properties();
        InputStream in = null;

        try {
            in = new FileInputStream(new File(CFG_FILE));
            properties.load(in);
            this.readRandom = Integer.parseInt(properties.getProperty(READ_TYPE), 10);
            this.testTime = Integer.parseInt(properties.getProperty(TEST_TIME), 10);
            this.pipeEnable = Integer.parseInt(properties.getProperty(TEST_PIPELINE), 10);
            String testThread = "redis.test.thread";
            this.threadNum = Integer.parseInt(properties.getProperty(testThread), 10);
            String tpsCountInterval = "redis.tps.count.interval";
            this.interval = Integer.parseInt(properties.getProperty(tpsCountInterval), 10);

            this.keyLen = Integer.parseInt(properties.getProperty(TEST_KEY_LEN), 10);
            this.valueLen = Integer.parseInt(properties.getProperty(TEST_VALUE_LEN), 10);
            this.fieldNum = Integer.parseInt(properties.getProperty(TEST_FIELD_NUM), 10);
            this.fieldLen = Integer.parseInt(properties.getProperty(TEST_FIELD_LEN), 10);

            this.sleepTime = Integer.parseInt(properties.getProperty("sleep.per.op"));
            this.cost2long = Integer.parseInt(properties.getProperty("cost.too.long", "300"));
        } catch (IOException e) {
            LOG.error("Failed to load redis test cfg file", e);
            try {
                in.close();
            } catch (IOException ignored) {

            }
        } finally {
            try {
                in.close();
            } catch (IOException ignored) {

            }

        }
        if (this.readRandom != 0) {
            this.readRandom = 1;
        }
        if (this.testTime < 0) {
            this.testTime = 0;
        }
        if (this.interval < 1) {
            this.interval = 5;
        }
        if (this.pipeEnable != 0) {
            this.pipeEnable = 1;

            this.threadNum = 1;

        }

        if (this.threadNum < 1) {

            this.threadNum = 1;

        }

        if (this.fieldNum < 1) {
            this.fieldNum = 1;
        }

        if (this.valueLen < 1) {
            this.valueLen = 1;
        }
        if (this.keyLen < 14) {
            this.keyLen = 14;
        }
        if (this.fieldLen < 10) {
            this.fieldLen = 10;
        }

    }

    public RedisPerform(String func, double startNum, double stopNum) {

        initClientCfg();

        this.func = func;

        this.startNum = startNum;

        this.stopNum = stopNum;

        this.rConn = new RedisConnector();

    }

    public void close() {

        if (this.rConn != null) {
            this.rConn.close();
        }

    }

    public static void main(String[] args) throws Exception {

        if ((args.length == 1) && ((args[0].equals("--version")) || (args[0].equals("-v")))) {
            System.out.println("version: " + VERSION);
            return;
        }
        if ((args.length != 2) && (args.length != 3)) {
            help();
            return;
        }
        double startNum = 0;
        double stopNum = 0;

        String func = args[0];

        String[] supportFunc = {"set", "get", "hmset", "hget", "rpush", "lrange", "sadd", "smembers", "zadd", "zrange"};

        if (!Arrays.asList(supportFunc).contains(func)) {

            System.out.println("This programe not support the command : " + func);

            return;

        }

        if (args.length == 3) {
            try {
                startNum = Double.parseDouble(args[1]);
                stopNum = Double.parseDouble(args[2]);

            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (args.length == 2) {
            try {
                startNum = 0;
                stopNum = Double.parseDouble(args[1]);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (stopNum <= startNum) {
            System.out.printf("stopNum(%.0f) must greater than startNum(%.0f)", stopNum, startNum);
            return;

        }

        RedisPerform test = new RedisPerform(func, startNum, stopNum);
        test.tester();
        test.close();

    }

    public static void help() {
        System.out.println(
            "java -jar *.jar set|get|hmset|hget|rpush|lrange|sadd|smembers|zadd|zrange [startNum] StopNum\n");

    }

    public void tester() {
        startThreads();
        checkThreads();

    }

    public void startThreads() {
        double stopNum = this.startNum;
        double packets = this.stopNum - this.startNum;
        for (int i = 0; i < this.threadNum; i++) {
            RedisPerformUnit t = new RedisPerformUnit(this.func, this.rConn);
            startNum = stopNum;
            stopNum = startNum + packets / this.threadNum;
            if (i == 0) {
                stopNum += packets % this.threadNum;

            }
            t.init(packets, startNum, stopNum, this.keyLen, this.fieldLen, this.valueLen, this.fieldNum,
                this.readRandom != 0, this.testTime != 0, this.pipeEnable != 0, this.cost2long);

            t.setSleepTime(this.sleepTime);
            t.start();
            this.threadList.add(t);

        }

    }

    public void checkThreads() {
        RedisPerformUnit tmp = null;

        AsyncSleep asyncSleep = new AsyncSleep(this.interval * 1000);

        asyncSleep.start();

        SimpleDateFormat fd = new SimpleDateFormat("yyyyMMdd-HHmmss");

        String csvName = String.format("%s/%s-%s.csv", LOG_PATH, fd.format(new Date()), this.func);

        FileWriter fp = null;

        try {
            fp = new FileWriter(csvName, true);
            fp.write(argsMsg());

        } catch (Exception e) {
            e.printStackTrace();

        }

        Counter tmpCounter = null;

        double[] totalTps = {0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D};

        String[] tmpMsg = new String[ROW_NUM];

        List<Counter> current = new LinkedList<>();

        List<Counter> last = new LinkedList<>();

        for (int i = 0; i < this.threadNum; i++) {
            tmp = this.threadList.get(i);
            tmpCounter = tmp.counter.getAll();
            current.add(tmpCounter);
            last.add(tmpCounter);

        }

        long times = 0L;
        long printTimes = 0L;
        long maxTimes = 9223372036854775807L;
        if (this.testTime != 0) {
            maxTimes = this.testTime * 60 / this.interval;

        }
        fd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long nowTime = System.currentTimeMillis();
        long lastTime = nowTime;
        LOG.info(String.format("%8s %10s %10s %10s %10s %10s %10s", "tps(k)", "read(k)", "read-err(k)", "write(k)", "write-err(k)", "latency", "cost2long"));
        int i = 0;
        while (times < maxTimes) {
            assert tmpCounter != null;
            tmpCounter.init();
            for (i = 0; i < this.threadNum; i++) {
                tmpCounter = Counter.add(Counter.dec(current.get(i), last.get(i)), tmpCounter);
            }
            if (!tmpCounter.isZero()) {
                times += 1L;

                if (times != 1L) {
                    double[] tps = tmpCounter.getTps(nowTime - lastTime);
                    for (i = 0; i < tps.length; i++) {
                        tmpMsg[i] = String.format("%10.2f", tps[i]);
                        totalTps[i] += tps[i];

                    }
                    printTimes += 1L;

                    LOG.info(
                        String.format("%8d %s", printTimes, KeyValue.join(tmpMsg, " ")));

                    try {
                        assert fp != null;
                        fp.write(String.format("%s,%s\n", fd.format(new Date(nowTime)),
                            KeyValue.join(tmpMsg, ",").replace(" ", "")));

                        if (times % 20L == 0L) {
                            fp.flush();
                        }

                    } catch (Exception ignored) {

                    }

                }

            }
            lastTime = nowTime;

            do {
                asyncSleep.set(true);
                nowTime = System.currentTimeMillis();

            } while (nowTime - lastTime < 500L);

            for (i = 0; i < this.threadNum; i++) {
                tmp = this.threadList.get(i);

                if (!tmp.isAlive()) {
                    times = maxTimes;
                    break;

                }

                last.set(i, current.get(i));

                current.set(i, tmp.counter.getAll());

            }

        }
        for (i = 0; i < totalTps.length; i++) {
            totalTps[i] /= printTimes;

        }
        for (i = 0; i < totalTps.length; i++) {
            tmpMsg[i] = String.format("%10.2f", totalTps[i]);

        }
        LOG.info(String.format("%8s %s", "average", KeyValue.join(tmpMsg, " ")));

        try {
            fp.write(String.format("%s,%s\n", "average", KeyValue.join(tmpMsg, ",").replace(" ", "")));

            fp.close();

        } catch (Exception localException2) {

        }
        if (this.testTime == 0) {
            if (this.threadNum > 1) {
                LOG.info("one thread has stopped, stop count tps and wait the other threads stop.");
            }
            int stopped = 0;
            while (stopped != this.threadNum) {
                stopped = 0;
                for (i = 0; i < this.threadNum; i++) {
                    tmp = this.threadList.get(i);
                    if (!tmp.isAlive()) {
                        stopped++;

                    }

                }
                asyncSleep.set(true);

            }

        } else {
            for (i = 0; i < this.threadNum; i++) {
                ((RedisPerformUnit) this.threadList.get(i)).setStop();

            }

        }

        asyncSleep.set(false);

    }

    public String argsMsg() {
        List<String> msg1 = new ArrayList<>();
        List<String> msg2 = new ArrayList<>();

        msg1.add(String.format("%8s", "testName"));
        msg2.add(String.format("%8s", this.func));

        msg1.add(String.format("%10s", "start"));
        msg2.add(String.format("%10s", this.startNum));
        msg1.add(String.format("%10s", "end"));
        msg2.add(String.format("%10s", this.stopNum));

        msg1.add(String.format("%6s", "keyLen"));
        msg2.add(String.format("%6s", this.keyLen));

        if (this.func.contains("h")) {
            msg1.add(String.format("%8s", "fieldLen"));
            msg2.add(String.format("%8s", this.fieldLen));

        }
        if (this.func.contains("set")) {
            msg1.add(String.format("%8s", "valueLen"));
            msg2.add(String.format("%8s", this.valueLen));

        }
        if (this.func.equals("hmset")) {
            msg1.add(String.format("%8s", "fieldNum"));
            msg2.add(String.format("%8s", this.fieldNum));

        }

        msg1.add(String.format("%8s", "interval"));

        msg2.add(String.format("%8s", this.interval));

        msg1.add(String.format("%4s", "pipe"));

        if (this.pipeEnable != 0) {
            msg2.add(String.format("%4s", "on"));

        } else {
            msg2.add(String.format("%4s", "off"));

            msg1.add(String.format("%6s", "thread"));

            msg2.add(String.format("%6s", this.threadNum));

        }

        if (this.func.contains("get")) {
            msg1.add(String.format("%6s", "random"));
            if (this.readRandom != 0) {
                msg2.add(String.format("%6s", "on"));
            } else {
                msg2.add(String.format("%6s", "off"));

            }

        }
        msg1.add(String.format("%4s", "time"));
        if (this.testTime != 0) {
            msg2.add(String.format("%3dm", this.testTime));
        } else {
            msg2.add(String.format("%4s", "off"));

        }

        String[] array = new String[msg1.size()];

        LOG.info(KeyValue.join(msg1.toArray(array), " "));

        LOG.info(KeyValue.join(msg2.toArray(array), " "));

        return String.format("%s\n%s\n", KeyValue.join((String[]) msg1.toArray(array), ",").replace(" ", ""),
            KeyValue.join((String[]) msg2.toArray(array), ",").replace(" ", ""));

    }

}

