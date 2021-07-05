/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.streaming.example.cql;

import com.huawei.streaming.cql.exception.CQLException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 读取文件中的CQL语句
 * 使用了Guava的接口
 *
 * @since 2020-08-22
 */
public class CQLFileReader implements LineProcessor<List<String>> {
    private static final String CHARSET_STRING = "UTF-8";

    private static final Charset CHARSET = Charset.forName(CHARSET_STRING);

    private static final Logger LOG = LoggerFactory.getLogger(CQLFileReader.class);

    private static final String CQL_LINE_SEPARATOR = ";";

    private StringBuilder sb = null;

    private Pattern pattern = null;

    /**
     * <默认构造函数>
     */
    public CQLFileReader() {
        sb = new StringBuilder();
        pattern = Pattern.compile("\t|\r|\n");
    }

    /**
     * 读取CQL文件
     *
     * @param file 待读取的文件
     * @throws CQLException 文件处理异常
     * @see [类、类#方法、类#成员]
     */
    public void readCQLs(String file) throws CQLException {
        validateCQLFile(file);
        readLines(file);
    }

    /**
     * 处理文件中的每行记录，将非注释行拼接起来
     */
    @Override
    public boolean processLine(String line) throws IOException {
        if (isEmpty(line)) {
            return true;
        }

        if (isCommentsLine(line.trim())) {
            LOG.debug("throw comments line {}", line);
            return true;
        }

        sb.append(line);
        return true;
    }

    /**
     * 获取解析好的CQL语句
     */
    @Override
    public List<String> getResult() {
        List<String> results = Lists.newArrayList();
        String[] cqls = replaceBlank(sb.toString()).split(CQL_LINE_SEPARATOR);
        for (String cql : cqls) {
            if (isEmpty(cql)) {
                continue;
            }

            results.add(cql);
        }

        return results;
    }

    /**
     * 是否是注释行
     */
    private boolean isCommentsLine(String newLine) {
        return newLine.startsWith("--") || newLine.startsWith("/*") || newLine.startsWith("*");
    }

    /**
     * 替换字符串中的空格等特殊符号
     */
    private String replaceBlank(String str) {
        String dest = "";
        if (str != null) {
            Matcher matcher = pattern.matcher(str);
            dest = matcher.replaceAll(" ");
        }
        return dest;
    }

    /**
     * 字符串是否为空
     *
     * @param str 字符串
     * @return 为空，返回true
     * @see [类、类#方法、类#成员]
     */
    private boolean isEmpty(String str) {
        if (Strings.isNullOrEmpty(str)) {
            return true;
        }

        if (Strings.isNullOrEmpty(str.trim())) {
            return true;
        }

        return false;
    }

    /**
     * 按行读取文件内容
     */
    private void readLines(String file) throws CQLException {
        try {
            Files.readLines(new File(file), CHARSET, this);
        } catch (IOException e) {
            LOG.error("failed to read cql file.", e);
            throw new CQLException("failed to read cql file.", e);
        }
    }

    /**
     * 校验CQL文件，检查文件是否存在等。
     */
    private void validateCQLFile(String file) throws CQLException {
        File cqlFile = new File(file);

        if (!cqlFile.exists()) {
            LOG.error("Invalid cql file, file does not exist.");
            throw new CQLException("Invalid cql file, file does not exist.");
        }

        if (!cqlFile.isFile()) {
            LOG.error("Invalid cql file, file {} does not file Type.");
            throw new CQLException("Invalid cql file, file {} does not file Type.");
        }
    }
}
