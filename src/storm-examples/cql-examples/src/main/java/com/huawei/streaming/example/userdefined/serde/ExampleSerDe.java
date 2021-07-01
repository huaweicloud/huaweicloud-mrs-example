/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.streaming.example.userdefined.serde;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;
import com.huawei.streaming.util.StreamingDataType;
import com.huawei.streaming.util.datatype.DataTypeParser;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 序列化和反序列化的例子
 *
 * @since 2020-08-22
 */
public class ExampleSerDe implements StreamSerDe {
    private static final long serialVersionUID = -8447913584480461044L;

    private static final Logger LOG = LoggerFactory.getLogger(ExampleSerDe.class);

    private TupleEventType schema;

    private String separator = ",";

    private StringBuilder sb = new StringBuilder();

    private DataTypeParser[] parsers;

    private StreamingConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException {
        this.config = conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamingConfig getConfig() {
        // 读取配置属性
        return config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSchema(TupleEventType outputSchema) {
        schema = outputSchema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleEventType getSchema() {
        return schema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize() throws StreamSerDeException {
        parsers = new DataTypeParser[schema.getSize()];
        Class<?>[] attributes = schema.getAllAttributeTypes();

        try {
            for (int i = 0; i < schema.getSize(); i++) {
                parsers[i] = StreamingDataType.getDataTypeParser(attributes[i], config);
            }
        } catch (StreamingException e) {
            LOG.error("Failed to create dataType parser instance.");
            throw new StreamSerDeException("Failed to create dataType parser instance.");
        }
    }

    /**
     * 反序列化
     * 将从输入流中读取的数据解析成系统可识别数据类型
     *
     * @param data 输入流中读取的数据。
     * 输入算子不同，反序列化类中的data数据类型也不同，
     * 一般是字符串类型
     * 在TcpServerInput中是byte[]
     * 在示例程序中，按照字符串进行解析
     * @return 解析好的数据
     * 由于可能从一行衍生出多行数据，所以返回值类型是数组类型
     * @throws StreamSerDeException 序列化异常
     * @see [类、类#方法、类#成员]
     */
    @Override
    public List<Object[]> deSerialize(Object data) throws StreamSerDeException {
        if (data == null) {
            return null;
        }

        String stringValue = (String) data;
        List<Object[]> splitResults = Lists.newArrayList();
        Object[] values = Lists.newArrayList(Splitter.on(separator).split(stringValue)).toArray();
        splitResults.add(values);
        return createEventsInstance(splitResults);
    }

    /**
     * 序列化方法
     * 将传入的事件进行序列化，转成output可以识别的对象
     * 或者字符串，或者其他类型
     *
     * @param events 系统产生或者处理过后的事件，是一个数组
     * @return 序列化完毕的数据，可以是字符串之类
     * @throws StreamSerDeException 序列化异常
     * @see [类、类#方法、类#成员]
     */
    @Override
    public Object serialize(List<Object[]> events) throws StreamSerDeException {
        if (events == null) {
            LOG.info("Input event is null");
            return null;
        }

        clearTmpString();

        for (Object[] event : events) {
            serializeEvent(event);
        }

        return removeLastChar();
    }

    /**
     * 根据schema中的数据列，生成对应的数据类型
     */
    private List<Object[]> createEventsInstance(List<Object[]> events) throws StreamSerDeException {
        if (events == null || events.size() == 0) {
            return Lists.newArrayList();
        }

        List<Object[]> list = Lists.newArrayList();
        for (Object[] event : events) {
            Object[] eventInstance = createEventInstance(event);
            list.add(eventInstance);
        }
        return list;
    }

    /**
     * 创建单个事件实例
     */
    private Object[] createEventInstance(Object[] event) throws StreamSerDeException {
        validateColumnSize(event);

        Object[] arr = new Object[schema.getAllAttributes().length];

        try {
            for (int i = 0; i < schema.getAllAttributeTypes().length; i++) {
                arr[i] = parsers[i].createValue(event[i].toString());
            }
        } catch (StreamingException e) {
            throw new StreamSerDeException(e.getMessage(), e);
        }
        return arr;
    }

    private String removeLastChar() {
        return sb.substring(0, sb.length() - 1);
    }

    /**
     * 序列化单行事件
     */
    private void serializeEvent(Object[] event) {
        for (Object column : event) {
            appendToTmpString(column);
        }
        sb.replace(sb.length() - 1, sb.length() - 1, "");
    }

    /**
     * 添加值到临时字符串最后面
     */
    private void appendToTmpString(Object val) {
        if (val != null) {
            sb.append(val.toString() + separator);
        } else {
            sb.append(separator);
        }
    }

    /**
     * 在序列化之前，先清空临时字符串
     */
    private void clearTmpString() {
        sb.delete(0, sb.length());
    }

    /**
     * 检查列的数量和schema中列的数量是否一致
     */
    private void validateColumnSize(Object[] columns) throws StreamSerDeException {
        if (columns.length != schema.getAllAttributeTypes().length) {
            LOG.error(
                    "deserializer result array size is not equal to the schema column size, "
                            + "schema size :{}, deserializer size :{}",
                    schema.getAllAttributeTypes().length,
                    columns.length);
            throw new StreamSerDeException(
                    "deserializer result array size is not equal to the schema column size, schema size :"
                            + schema.getAllAttributeTypes().length
                            + ", deserializer size :"
                            + columns.length);
        }
    }
}
