/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.streaming.example.userdefined.datasource;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.datasource.IDataSource;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

/**
 * 属性匹配数据源
 * 作为示例，从本地文件中获取配置文件
 * 读取配置文件中的key，value对应关系
 * 返回对应的查询值
 *
 * @since 2020-08-22
 */
public class PropertyMatchDataSource implements IDataSource {
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private static final long serialVersionUID = 8056232432674642637L;

    private static final Logger LOG = LoggerFactory.getLogger(PropertyMatchDataSource.class);

    private static final String CONF_FILE_PATH = "example.datasource.path";

    private String propertyFilePath;

    private Properties properties;

    private StreamingConfig config;

    /**
     * 这里的schema，对应查询语句中的schema定义，指的是数据源查询的输出schema
     * 在这个示例中并没有用到
     */
    private TupleEventType schema;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException {
        config = conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSchema(TupleEventType tuple) {
        schema = tuple;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize() throws StreamingException {
        initConfig();

        if (Strings.isNullOrEmpty(propertyFilePath)) {
            LOG.error("file path is null.");
            throw new StreamingException("file path is null.");
        }

        File file = new File(propertyFilePath);
        validateFile(file);
        loadProperties(file);
    }

    private void initConfig() throws StreamingException {
        // 初始化，用于读取配置属性
        if (config.containsKey(CONF_FILE_PATH)) {
            this.propertyFilePath = config.get(CONF_FILE_PATH).toString();
        } else {
            LOG.error("can not found {} from configuration.", CONF_FILE_PATH);
            throw new StreamingException(ErrorCode.CONFIG_NOT_FOUND, CONF_FILE_PATH);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Object[]> execute(List<Object> replacedQueryArguments) throws StreamingException {
        validateArgs(replacedQueryArguments);
        return evaluateValue(replacedQueryArguments.get(0));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws StreamingException {
        // 用作打开资源的销毁，比如关闭流，关闭连接等

    }

    private List<Object[]> evaluateValue(Object replacedQueryArgument) {
        String key = replacedQueryArgument.toString();

        if (properties.containsKey(key)) {
            Object[] values = {properties.get(key)};
            List<Object[]> results = Lists.newArrayList();
            results.add(values);
            return results;
        }

        return null;
    }

    private void validateFile(File file) throws StreamingException {
        if (!file.exists()) {
            LOG.error("file in path is not exists.");
            throw new StreamingException("file in path is not exists.");
        }

        if (!file.isFile()) {
            LOG.error("file in path is not a file type.");
            throw new StreamingException("file in path is not a file type.");
        }
    }

    private void loadProperties(File file) throws StreamingException {
        properties = new Properties();
        BufferedReader reader = null;
        try {
            reader = Files.newReader(file, CHARSET);
            properties.load(Files.newReader(file, CHARSET));
        } catch (IOException e) {
            LOG.error("failed to read property files.", e);
            throw new StreamingException("failed to read property files.");
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    private void validateArgs(List<Object> replacedQueryArguments) throws StreamingException {
        if (replacedQueryArguments.size() != 1) {
            LOG.error(
                    "rdb dataSource query arguments are not equal to 1, args size : {}", replacedQueryArguments.size());
            throw new StreamingException(
                    "rdb dataSource query arguments are not equal to 1, args size : " + replacedQueryArguments.size());
        }
    }
}
