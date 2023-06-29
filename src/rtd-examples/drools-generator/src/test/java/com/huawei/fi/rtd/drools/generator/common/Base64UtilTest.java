package com.huawei.fi.rtd.drools.generator.common;

import org.junit.Assert;
import org.junit.Test;

public class Base64UtilTest {

    @Test
    public void input_string_encode_base64_and_compare_equals() {

        String hello = "hello, world";
        String encode = Base64Util.encode(hello);

        Assert.assertTrue(encode.equals("aGVsbG8sIHdvcmxk"));
    }

    @Test
    public void input_encoded_string_and_decode_then_compare_equals() {

        String encode = "aGVsbG8sIHdvcmxk";
        String hello = Base64Util.decode(encode);

        Assert.assertTrue(hello.equals("hello, world"));
    }
}
