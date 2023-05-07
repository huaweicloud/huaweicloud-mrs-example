/*
 *  Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fi.rtd.drools.generator;

import com.huawei.fi.rtd.drools.generator.builder.DroolsKModuleFileBuilder;
import com.huawei.fi.rtd.drools.generator.builder.DroolsRuntimeFileBuilder;
import com.huawei.fi.rtd.drools.generator.builder.RtdRuntimeMetaFileBuilder;
import com.huawei.fi.rtd.drools.generator.common.DroolsGeneratorUtil;
import com.huawei.fi.rtd.drools.generator.common.ZipFileUtil;
import com.huawei.fi.rtd.drools.generator.exception.DroolsGeneratorException;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * <一句话功能简述>
 * <功能详细描述>
 *
 * @version [版本号, 2018年1月10日]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class RtdDroolsFileGenerator {
    private static final Logger logger = LoggerFactory.getLogger(RtdDroolsFileGenerator.class);

    public static final RtdDroolsFileGenerator INSTANCE = new RtdDroolsFileGenerator();

    private static final String RTD_META_DATA_PROPERTIES = "rtd_meta_data.properties";

    private static final String RTD_DROOLS_DECISION_JSON = "drools_contract.json";

    private static final String RTD_DROOLS_K_MODULE_XML = "kmodule.xml";

    private RtdDroolsFileGenerator() {
    }

    /**
     * 传入契约字符串，输出文件到指定文件夹
     *
     * @param contractJson 传入字符串
     * @param outputDirectoryPath 输出路径
     * @param fileName 文件名
     * @return 输出的file对象
     */
    public File outputZipFile(String contractJson, String outputDirectoryPath, String fileName) {
        DroolsGeneratorUtil.isValidDirectoryPath(outputDirectoryPath);
        RtdDroolsDecision ruleDecision = JSONObject.parseObject(contractJson, RtdDroolsDecision.class);
        return this.outputZipFile(ruleDecision, outputDirectoryPath, fileName);
    }

    /**
     * 传入契约字符串，输出缓冲读取流
     * 注意：外围负责关闭
     *
     * @param contractJson 传入字符串
     * @return 流对象
     */
    public ByteArrayOutputStream outputZipStream(String contractJson) {
        RtdDroolsDecision ruleDecision = JSONObject.parseObject(contractJson, RtdDroolsDecision.class);
        return this.outputZipStream(ruleDecision);
    }

    /**
     * 传入规则决策对象，输出文件到指定文件夹
     *
     * @param ruleDecision 规则决策对象
     * @param outputDirectoryPath 输出路径
     * @param fileName 文件名
     * @return 输出的file对象
     */
    public File outputZipFile(RtdDroolsDecision ruleDecision, String outputDirectoryPath, String fileName) {
        DroolsGeneratorUtil.isValidDirectoryPath(outputDirectoryPath);
        RtdDroolsFileGenerator.this.isValidZipSuffix(fileName);

        File outputFile = new File(outputDirectoryPath + File.separator + fileName);
        Map<String, String> fileMap = getOutputFileContentMap(ruleDecision);

        logger.debug("output zip file: \n{}", fileMap);
        if (! ZipFileUtil.outputZipFile(outputFile, fileMap)) {
            throw new DroolsGeneratorException("Create zip file fail ....");
        }
        return outputFile;
    }

    /**
     * 传入规则决策对象，输出缓冲读取流
     * 注意：外围负责关闭
     *
     * @param ruleDecision 决策对象
     * @return 缓冲读取流
     */
    public ByteArrayOutputStream outputZipStream(RtdDroolsDecision ruleDecision) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Map<String, String> fileMap = getOutputFileContentMap(ruleDecision);

        logger.debug("output zip stream: \n{}", fileMap);
        if (!ZipFileUtil.outputZipStream(baos, fileMap)) {
            throw new DroolsGeneratorException("Create zip file stream fail ....");
        }
        return baos;
    }

    /**
     * 判断是否是合法的zip文件扩展名
     *
     * @param fileName 文件扩展名
     * @return 名称是否合法
     */
    private boolean isValidZipSuffix(String fileName) {
        return StringUtils.isNotBlank(fileName) && fileName.endsWith(".zip");
    }

    /**
     * 生成待输出成文件的文件内容，并输出到map中
     *
     * @param ruleDecision 文件内容
     * @return 生成的map
     */
    private Map<String, String> getOutputFileContentMap(RtdDroolsDecision ruleDecision) {
        Map<String, String> fileMap = new HashMap<>();

        fileMap.put(RTD_DROOLS_K_MODULE_XML, new DroolsKModuleFileBuilder().buildXmlFile());
        fileMap.put(RTD_META_DATA_PROPERTIES, new RtdRuntimeMetaFileBuilder().buildMetaFile(ruleDecision));
        fileMap.put(RTD_DROOLS_DECISION_JSON, JSONObject.toJSONString(ruleDecision, SerializerFeature.PrettyFormat));

        fileMap.putAll(new DroolsRuntimeFileBuilder().buildDrlFile(ruleDecision));
        return fileMap;
    }
}
