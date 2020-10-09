/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.streaming.example.userdefined.operator.input;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IInputStreamOperator;
import com.huawei.streaming.serde.StreamSerDe;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * 文件读取示例
 *
 * @since 2020-08-22
 */
public class FileInput implements IInputStreamOperator {
    private static final long serialVersionUID = 1145305812403368160L;

    private static final Logger LOG = LoggerFactory.getLogger(FileInput.class);

    private static final String CONF_FILE_PATH = "fileinput.path";

    private static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * 文件路径
     */
    private String filePath;

    private IEmitter emitter;

    private StreamSerDe serde;

    private StreamingConfig config;

    /**
     * file在initialize接口中被实例化
     * 在运行时被调用
     * 所以不用进行序列化传输
     */
    private transient File file;

    private transient FileLineProcessor processor;

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
    public void setEmitter(IEmitter iEmitter) {
        this.emitter = iEmitter;
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
    public void initialize() throws StreamingException {
        if (Strings.isNullOrEmpty(filePath)) {
            LOG.error("file path is null.");
            throw new StreamingException("file path is null.");
        }

        file = new File(filePath);

        if (!file.exists()) {
            LOG.error("file in path is not exists.");
            throw new StreamingException("file in path is not exists.");
        }

        if (!file.isFile()) {
            LOG.error("file in path is not a file type.");
            throw new StreamingException("file in path is not a file type.");
        }

        processor = new FileLineProcessor();
    }

    /**
     * 运行时的销毁接口
     *
     * @throws StreamingException 流处理异常
     * @see [类、类#方法、类#成员]
     */
    @Override
    public void destroy() throws StreamingException {}

    /**
     * 输入算子执行接口
     *
     * @throws StreamingException 流处理异常
     * @see [类、类#方法、类#成员]
     */
    @Override
    public void execute() throws StreamingException {
        try {
            Files.readLines(file, CHARSET, processor);
        } catch (IOException e) {
            throw StreamingException.wrapException(e);
        }
    }

    private class FileLineProcessor implements LineProcessor<Object> {
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean processLine(String line) throws IOException {
            List<Object[]> deserResults = null;
            try {
                deserResults = serde.deSerialize(line);
            } catch (StreamSerDeException e) {
                // 忽略反序列化异常
                LOG.warn("Ignore a serde exception.", e);
                return false;
            }

            for (Object[] event : deserResults) {
                try {
                    emitter.emit(event);
                } catch (StreamingException e) {
                    // emit异常直接抛出，由外部异常处理机制自己处理
                    throw new StreamingRuntimeException(e);
                }
            }

            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object getResult() {
            return null;
        }
    }
}
