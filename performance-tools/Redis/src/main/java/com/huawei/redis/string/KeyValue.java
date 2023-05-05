/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.string;

import redis.clients.jedis.util.JedisClusterCRC16;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class KeyValue {

    public static Map<String, String> getHashValue(int fieldNum, int fieldLen, int valueLen) {
        Map hash = new HashMap();
        for (int i = 0; i < fieldNum; i++) {
            hash.put(getHashfield(fieldLen, i, false), getString(valueLen, true));

        }
        return hash;

    }

    public static Map<String, Double> getHashDoubleValue(int fieldNum, int fieldLen, int valueLen) {

        Map hash = new HashMap();

        for (int i = 0; i < fieldNum; i++) {

            hash.put(getString(valueLen, true), (double) i);

        }

        return hash;

    }

    public static String getValueWithIndex(int length, int index) {

        return String.format("%s_%d", getString(length, false), index);

    }

    public static String getKey(double keyLen, double index, boolean isRandom) {

        return String.format("key_%s%.0f", getString(keyLen - 14, isRandom), index);

    }

    public static String getKeyWithSlot(double keyLen, double index, boolean isRandom) {

        String key = getKey(keyLen, index, isRandom);

        return String.format("%s[%d]", key, JedisClusterCRC16.getSlot(key));

    }

    public static String getHashfield(double fieldLen, double index, boolean isRandom) {

        return String.format("field_%skey_%.0f", getString(fieldLen - 10, isRandom), index);

    }

    public static String getString(double length, boolean isRandom) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder buf = new StringBuilder();
        if (isRandom) {
            Random random = new Random();
            for (int i = 0; i < length; i++) {
                buf.append(str.charAt(random.nextInt(62)));
            }

        } else {
            for (int i = 0; i < length; i++) {
                buf.append(str.charAt(i % str.length()));

            }

        }
        return buf.toString();

    }

    public static String join(String[] strArray, String s) {

        if (strArray.length < 1) {

            return "";

        }

        int i = 0;

        StringBuilder buf = new StringBuilder();

        for (i = 0; i < strArray.length - 1; i++) {

            buf.append(strArray[i]).append(s);

        }

        buf.append(strArray[i]);

        return buf.toString();

    }

}

