/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.oozie.examples;

/**
 * Oozie Example for Normal Mode using Java API
 *
 * @since 2020-09-30
 */
public class OozieMainForNormal {
    private static boolean UNSECURTY_CLUSTER = false;

    public static void main(String[] args) {
        try {
            OozieSample oozieSample = new OozieSample(UNSECURTY_CLUSTER);
            oozieSample.test();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("-----------finish Oozie -------------------");
    }
}
