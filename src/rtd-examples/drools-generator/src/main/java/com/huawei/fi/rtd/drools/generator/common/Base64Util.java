package com.huawei.fi.rtd.drools.generator.common;

import org.apache.commons.codec.binary.Base64;

/*
 * 文 件 名:  BASE64Util.java
 * 版    权:  Huawei Technologies Co., Ltd. Copyright YYYY-YYYY,  All rights reserved
 * 描    述:  <描述>
 * 修 改 人:
 * 修改时间:  2015年4月29日
 * 跟踪单号:  <跟踪单号>
 * 修改单号:  <修改单号>
 * 修改内容:  <修改内容>
 */
public class Base64Util {

    /**
     * <一句话功能简述>
     * <功能详细描述>
     * @param s s
     * @return String
     * @see [类、类#方法、类#成员]
     */
    public static String encode(String s) {

        return s == null ? null: Base64.encodeBase64String(s.getBytes());
    }
    
    /**
     * <一句话功能简述>
     * <功能详细描述>
     * @param s s
     * @return String
     * @see [类、类#方法、类#成员]
     */
    public static String decode(String s) {

        return s == null ? null: new String(Base64.decodeBase64(s));
    }
}
