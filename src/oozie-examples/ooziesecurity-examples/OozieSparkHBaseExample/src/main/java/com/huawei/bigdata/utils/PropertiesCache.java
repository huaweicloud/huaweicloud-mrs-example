/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

/**
 * properties util
 *
 * @since 2021-01-25
 */
public class PropertiesCache {
    private final Properties configProp = new Properties();

    private PropertiesCache() {
        // Private constructor to restrict new instances
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("application.properties")) {
            configProp.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // singleton pattern
    private static class LazyHolder {
        private static final PropertiesCache INSTANCE = new PropertiesCache();
    }

    public String getProperty(String key){
        return configProp.getProperty(key);
    }

    public static PropertiesCache getInstance() {
        return LazyHolder.INSTANCE;
    }
}
