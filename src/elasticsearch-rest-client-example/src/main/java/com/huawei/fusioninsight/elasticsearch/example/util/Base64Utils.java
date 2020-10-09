/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * base64编码工具
 *
 * @since 2020-09-30
 */
public class Base64Utils {
    private static final Logger LOG = LogManager.getLogger(Base64Utils.class);

    public static void main(String[] args) {
        // 对字符串"userName:passwd"进行base64加密
        LOG.info("encodeBase64: {}", encodeBase64("userName:passwd"));
        // 对已加密的字符串"dXNlck5hbWU6cGFzc3dk"进行base64解密
        LOG.info("decodeBase64 {}", decodeBase64("dXNlck5hbWU6cGFzc3dk"));
    }

    /**
     * base64加密
     *
     * @param needEncodeString 需要加密的内容
     * @return 加密结果
     */
    private static String encodeBase64(String needEncodeString) {
        return Base64.encodeBase64String(needEncodeString.getBytes());
    }

    /**
     * base64解密
     *
     * @param needDecodeBase64Str 需要解密的内容
     * @return 解密结果
     */
    private static String decodeBase64(String needDecodeBase64Str) {
        byte[] result = Base64.decodeBase64(needDecodeBase64Str.getBytes());
        return new String(result);
    }
}
