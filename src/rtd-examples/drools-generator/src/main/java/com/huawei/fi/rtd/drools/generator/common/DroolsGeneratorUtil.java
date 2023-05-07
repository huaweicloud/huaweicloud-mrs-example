/*
 *  Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fi.rtd.drools.generator.common;

import com.huawei.fi.rtd.drools.generator.exception.DroolsGeneratorException;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DroolsGeneratorUtil {

    private static final String RESERVED_KEYWORDS_FOR = "for";

    private static final String RESERVED_KEYWORDS_WHILE = "while";

    private static final String JAVA_LOOP_WHILE_PATTERN_REG = RESERVED_KEYWORDS_WHILE + "(\\s|\t|\n)*\\(";

    private static final String JAVA_LOOP_FOR_PATTERN_REG = RESERVED_KEYWORDS_FOR
            + "(\\s|\t|\n)*\\(([\\S\\s]*?;{1})([\\S\\s]*?;{1})";

    private static final String FUNCTION_PATTERN_REG = "^function double\\s+(\t|\r|\n)*([A-Za-z_$]+[A-Za-z_$\\d]+)(\\s|\t|\n)*\\(";

    private static Pattern functionPattern = Pattern.compile(FUNCTION_PATTERN_REG);

    private static Pattern forLoopPattern = Pattern.compile(JAVA_LOOP_FOR_PATTERN_REG);

    /**
     * 判断是否是合法的算法名称
     *
     * @param algorithmName 算法名称
     * @return 是否合法
     */
    public static boolean isValidAlgorithm(String algorithmName) {
        long match = Arrays.asList(AlgorithmType.values()).stream()
                .filter(f -> f.name().equalsIgnoreCase(algorithmName)).count();
        if (match > 0) {
            return true;
        }
        throw new DroolsGeneratorException("Unsupported algorithm type ....");
    }

    /**
     * 判断是否是合法的自定义函数代码
     *
     * @param customizeCode
     * @param algorithmName
     * @return
     */
    public static boolean isValidCustomizeCode(String customizeCode, String algorithmName) {
        boolean isCustomizeAlgorithm = AlgorithmType.CUSTOMIZE.name().equalsIgnoreCase(algorithmName);
        if (isCustomizeAlgorithm
                && StringUtils.isNotBlank(DroolsGeneratorUtil.getCustomizeFunctionName(customizeCode))) {
            return true;
        }
        if (!isCustomizeAlgorithm && StringUtils.isBlank(customizeCode)) {
            return true;
        }
        throw new DroolsGeneratorException("Invalid customize code ....");
    }

    /**
     * 从客户个性化代码中，获取自定义函数名称
     *
     * @param customizeCode 客户代码
     * @return 自定义函数名称
     */
    public static String getCustomizeFunctionName(String customizeCode) {
        if (StringUtils.isBlank(customizeCode)) {
            throw new DroolsGeneratorException("Customize code can not null ....");
        }
        Matcher matcher = functionPattern.matcher(customizeCode);
        while (matcher.find()) {
            String matchString = matcher.group(2);
            if (StringUtils.isBlank(matchString)) {
                throw new DroolsGeneratorException("Invalid customize function name ....");
            }
            // 强行限制使用for|while关键字结尾的函数名称，避免与超时控制的代码相混淆
            if (matchString.endsWith(RESERVED_KEYWORDS_FOR) || matchString.endsWith(RESERVED_KEYWORDS_WHILE)) {
                throw new DroolsGeneratorException("Function name can not end with 'for' or 'while' keywords ....");
            }
            return matchString;
        }
        throw new DroolsGeneratorException("Invalid customize function name ....");
    }

    /**
     * 若用户自定义代码中包含while循环语句，则主动添加超时控制，避免存在死循环
     *
     * @param customizeCode 客户代码
     * @param controlCode 超时控制代码
     * @return 若用户自定义代码中包含while循环语句，返回添加超时控制的代码
     */
    public static String addTimeoutControl4WhileLoop(String customizeCode, String controlCode) {
        return customizeCode.replaceAll(JAVA_LOOP_WHILE_PATTERN_REG, RESERVED_KEYWORDS_WHILE + " (" + controlCode);
    }

    /**
     * 若用户自定义代码中包含for循环语句，则主动添加超时控制，避免存在死循环
     *
     * @param customizeCode 客户代码
     * @param controlCode 超时控制代码
     * @return 若用户自定义代码中包含for循环语句，返回添加超时控制的代码
     */
    public static String addTimeoutControl4ForLoop(String customizeCode, String controlCode) {
        StringBuilder result = new StringBuilder();
        Matcher matcher = forLoopPattern.matcher(customizeCode);

        int headIndex = 0;
        while (matcher.find()) {
            String targetString = matcher.group(3);
            int targetStart = matcher.start(3);
            int targetEnd = matcher.end(3);

            result.append(customizeCode.substring(headIndex, targetStart));
            result.append(controlCode + targetString);
            headIndex = targetEnd;
        }
        result.append(customizeCode.substring(headIndex));
        return result.toString();
    }

    /**
     * 判断是否是合法的组间规则优先级
     *
     * @param salience 优先级
     * @return 是否合法，true合法
     */
    public static boolean isValidGroupSalience(int salience) {
        if (salience > 0 && salience < 1000) {
            return true;
        }
        throw new DroolsGeneratorException("Invalid group salience [1, 999] ....");
    }

    /**
     * 判断是否是合法的最终输出规则的优先级
     *
     * @param salience 优先级
     * @return 是否合法，true合法
     */
    public static boolean isValidDecisionSalience(int salience) {
        if (salience == 0) {
            return true;
        }
        throw new DroolsGeneratorException("Decision salience must be 0 ....");
    }

    /**
     * 校验文件输出文件的合法性。
     *
     * @param outputDirectoryPath 输出文件路径
     * @return 是否合法，true合法
     */
    public static boolean isValidDirectoryPath(String outputDirectoryPath) {
        if (StringUtils.isBlank(outputDirectoryPath)) {
            throw new DroolsGeneratorException("Invalid output file path ....");
        }
        File outputFile = new File(outputDirectoryPath);
        try {
            File canonicalFile = outputFile.getCanonicalFile();
            return canonicalFile.exists() && canonicalFile.isDirectory();
        } catch (IOException e) {
            throw new DroolsGeneratorException("Invalid output directory path ....");
        }
    }
}
