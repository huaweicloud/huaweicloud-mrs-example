/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.utils;

import java.io.File;
import java.util.Objects;

/**
 * helper util
 *
 * @since 2021-01-25
 */
public class Helper {
    /** get absolute path to file in resources folder */
    public static String getResourcesPath() {
        return new Object() {
            public String getPath() {
                return Objects.requireNonNull(this.getClass().getClassLoader().getResource(".")).getPath();
            }
        }.getPath().substring(1).replace("/", File.separator);
    }
}
