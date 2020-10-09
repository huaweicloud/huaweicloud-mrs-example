/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.streaming.example.userdefined.operator;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IFunctionStreamOperator;

import com.google.common.io.Files;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;

/**
 * 用户自定义算子
 * 如果发送的数据在文件中存在，则直接替换成文件内容
 * 输入schema：a string, b int
 * 输出schema：c string, d int, e float
 *
 * @since 2020-08-22
 */
public class UserOperator implements IFunctionStreamOperator {
    private static final Logger LOG = LoggerFactory.getLogger(UserOperator.class);

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private static final long serialVersionUID = -4438239751340766284L;

    private static final String CONF_FILE_NAME = "userop.filename";

    private String fileName;

    private Properties properties;

    private Map<String, IEmitter> emitters = null;

    private StreamingConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException {
        if (!conf.containsKey(CONF_FILE_NAME)) {
            LOG.error("can not found config value {}.", CONF_FILE_NAME);
            throw new StreamingException("can not found config value " + CONF_FILE_NAME + ".");
        }

        fileName = conf.getStringValue(CONF_FILE_NAME);
        this.config = conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamingConfig getConfig() {
        return this.config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEmitter(Map<String, IEmitter> emitterMap) {
        if (emitterMap == null || emitterMap.isEmpty()) {
            LOG.error("can not found emitter.");
            throw new StreamingRuntimeException("can not found config value " + CONF_FILE_NAME + ".");
        }
        emitters = emitterMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize() throws StreamingException {
        File file = new File(fileName);
        validateFile(file);
        loadProperties(file);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event) throws StreamingException {
        Object[] values = event.getAllValues();
        Object[] result = new Object[3];

        if (properties.containsKey(String.valueOf(values[0]))) {
            result[0] = properties.get(String.valueOf(values[0]));
            result[1] = 1;
            result[2] = 1.0f;
        } else {
            result[0] = "NONE";
            result[1] = 1;
            result[2] = 1.0F;
        }

        for (IEmitter emitter : emitters.values()) {
            emitter.emit(result);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws StreamingException {}

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
            ;
        }
    }
}
