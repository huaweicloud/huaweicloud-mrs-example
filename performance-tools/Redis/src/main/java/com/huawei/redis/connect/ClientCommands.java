/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis.connect;

import java.util.List;
import java.util.Map;
import java.util.Set;

interface ClientCommands {
    String get(String paramString, boolean paramBoolean) throws Exception;

    String set(String paramString1, String paramString2, boolean paramBoolean) throws Exception;

    String hget(String paramString1, String paramString2, boolean paramBoolean) throws Exception;

    String hmset(String paramString, Map<String, String> paramMap, boolean paramBoolean) throws Exception;

    Long rpush(String paramString1, String paramString2, boolean paramBoolean) throws Exception;

    List<String> lrange(String paramString, Long paramLong1, Long paramLong2, boolean paramBoolean) throws Exception;

    Long sadd(String paramString1, String paramString2, boolean paramBoolean) throws Exception;

    Set<String> smembers(String paramString, boolean paramBoolean) throws Exception;

    Long zadd(String paramString, Map<String, Double> paramMap, boolean paramBoolean) throws Exception;

    Set<String> zrange(String paramString, Long paramLong1, Long paramLong2, boolean paramBoolean) throws Exception;

    Boolean exists(String paramString, boolean paramBoolean) throws Exception;

    List<Object> syncAndReturnAll() throws Exception;

    void close();
}
