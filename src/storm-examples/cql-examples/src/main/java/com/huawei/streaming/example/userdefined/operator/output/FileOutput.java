/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.streaming.example.userdefined.operator.output;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IOutputStreamOperator;
import com.huawei.streaming.serde.BaseSerDe;
import com.huawei.streaming.serde.StreamSerDe;

import com.google.common.base.Strings;
import com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * 用户自定义接口示例
 * 文件输出示例
 *
 * @since 2020-08-22
 */
public class FileOutput implements IOutputStreamOperator {
    private static final long serialVersionUID = 153729627538127379L;

    private static final Logger LOG = LoggerFactory.getLogger(FileOutput.class);

    private static final String CONF_FILE_PATH = "fileOutput.path";

    private static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * 文件路径
     */
    private String filePath;

    private StreamSerDe serde;

    private StreamingConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException {
        this.filePath = conf.getStringValue(CONF_FILE_PATH);
        this.config = conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSerDe(StreamSerDe streamSerDe) {
        this.serde = streamSerDe;
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
    public StreamSerDe getSerDe() {
        return this.serde;
    }

    /**
     * 初始化
     *
     * @throws StreamingException 初始化异常
     */
    @Override
    public void initialize() throws StreamingException {}

    /**
     * 执行
     *
     * @param event 事件数据
     */
    @Override
    public void execute(String streamName, TupleEvent event) throws StreamingException {
        if (Strings.isNullOrEmpty(filePath)) {
            LOG.error("file path is null.");
            throw new StreamingException("file path is null.");
        }

        File file = new File(filePath);

        if (!file.exists()) {
            LOG.error("file in path is not exists.");
            throw new StreamingException("file in path is not exists.");
        }

        if (!file.isFile()) {
            LOG.error("file in path is not a file type.");
            throw new StreamingException("file in path is not a file type.");
        }

        String result = serializeEvent(event);
        writeEvent(file, result);
    }

    /**
     * 运行时的销毁接口
     *
     * @throws com.huawei.streaming.exception.StreamingException 流处理异常
     * @see [类、类#方法、类#成员]
     */
    @Override
    public void destroy() throws StreamingException {}

    private String serializeEvent(TupleEvent event) {
        String result = null;
        try {
            result = (String) serde.serialize(BaseSerDe.changeEventsToList(event));
        } catch (StreamSerDeException e) {
            LOG.error("failed to serialize data ", e);
        }
        return result;
    }

    private void writeEvent(File file, String result) throws StreamingException {
        try {
            Files.append(result, file, CHARSET);
        } catch (IOException e) {
            throw StreamingException.wrapException(e);
        }
    }
}
