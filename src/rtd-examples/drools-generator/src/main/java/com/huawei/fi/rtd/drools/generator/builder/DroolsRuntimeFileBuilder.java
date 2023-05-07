package com.huawei.fi.rtd.drools.generator.builder;

import com.huawei.fi.rtd.drools.generator.RtdDroolsDecision;
import com.huawei.fi.rtd.drools.generator.RtdDroolsElement;
import com.huawei.fi.rtd.drools.generator.RtdDroolsElementsGroup;
import com.huawei.fi.rtd.drools.generator.common.AlgorithmType;
import com.huawei.fi.rtd.drools.generator.common.Base64Util;
import com.huawei.fi.rtd.drools.generator.common.DefinedFunctionType;
import com.huawei.fi.rtd.drools.generator.common.DroolsGeneratorUtil;
import com.huawei.fi.rtd.drools.generator.exception.DroolsGeneratorException;
import com.huawei.fi.rtd.drools.generator.template.DroolsDecisionRule;
import com.huawei.fi.rtd.drools.generator.template.DroolsFileHeader;
import com.huawei.fi.rtd.drools.generator.template.DroolsGroupRule;
import com.huawei.fi.rtd.drools.generator.template.DroolsInnerFunction;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class DroolsRuntimeFileBuilder {
    private static final String RTD_DROOLS_FILE_PREFIX = "rtd_drools_decision_";

    private static final String RTD_DROOLS_FILE_SUFFIX = ".drl";

    private static final int RTD_DROOLS_MAX_GROUP_PER_FILE = 20;

    private static final String GLOBAL_META_DATA = "metaData";

    private static final String INPUT_EXTENDER = "extender";

    private static final String INPUT_DROOLS_RESULT = "droolsResult";

    private static final String INPUT_DECISION_MAP = "decisionMap";

    private static final String INPUT_START_TIME = "startTime";

    private static final String INPUT_TIMEOUT = "timeout";

    private static final String NEXT_ONE_LINE = "\n";

    private static final String NEXT_TWO_LINE = "\n\n";

    private Set<String> customizeFunctionNames = new HashSet<>();

    /**
     * 根据用户传入的信息，生成.drl文件中的代码信息
     *
     * @param droolsDecision droolsDecision
     * @return 文件信息
     */
    public Map<String, String> buildDrlFile(RtdDroolsDecision droolsDecision) {
        this.isValidDroolsDecision(droolsDecision);
        StringBuilder sb = new StringBuilder(buildDrlFileHeader());
        sb.append(buildInnerFunction());
        sb.append(buildDecisionRule(droolsDecision));

        boolean isNeedTail = true;
        int elementGroupNumber = 0;
        Map<String, String> droolsCodeMap = new HashMap<>();
        List<RtdDroolsElementsGroup> elementGroups = droolsDecision.getElementsGroups();
        for (RtdDroolsElementsGroup group : elementGroups) {
            ++elementGroupNumber;
            sb.append(buildSingleGroupRule(group));
            if (elementGroupNumber % RTD_DROOLS_MAX_GROUP_PER_FILE == 0) {
                droolsCodeMap.put(getDrlFileName(elementGroupNumber), sb.toString());
                sb = new StringBuilder(buildDrlFileHeader());
                if (elementGroups.size() == elementGroupNumber) {
                    isNeedTail = false;
                }
            }
        }

        if (isNeedTail) {
            droolsCodeMap.put(getDrlFileName(++elementGroupNumber), sb.toString());
        }

        return droolsCodeMap;
    }

    /**
     * 构造drl文件头
     *
     * @return header
     */
    private String buildDrlFileHeader() {
        String[] headerParams = {GLOBAL_META_DATA, INPUT_DROOLS_RESULT, INPUT_EXTENDER};
        String headerCode = new DroolsFileHeader().render(headerParams);
        return headerCode;
    }

    /**
     * 构造决策层的drl规则
     *
     * @param droolsDecision droolsDecision
     * @return drl规则
     */
    private String buildDecisionRule(RtdDroolsDecision droolsDecision) {
        String decisionName = droolsDecision.getDecisionName();
        if (StringUtils.isBlank(decisionName)) {
            throw new DroolsGeneratorException("Decision name is null");
        }

        DroolsGeneratorUtil.isValidDecisionSalience(droolsDecision.getSalience());
        String salience = Integer.toString(droolsDecision.getSalience());

        String algorithmName = droolsDecision.getAlgorithm();
        DroolsGeneratorUtil.isValidAlgorithm(algorithmName);

        String customizeCode = Base64Util.decode(droolsDecision.getCustomizeCode());
        DroolsGeneratorUtil.isValidCustomizeCode(customizeCode, algorithmName);

        boolean isCustomize = AlgorithmType.CUSTOMIZE.name().equalsIgnoreCase(algorithmName);
        String functionName = isCustomize ?
                DroolsGeneratorUtil.getCustomizeFunctionName(customizeCode) : algorithmName.toLowerCase(Locale.ROOT);
        this.hasDuplicateFunctionName(customizeFunctionNames, isCustomize, functionName);

        String weight = isCustomize ? null : Double.toString(droolsDecision.getWeight());
        String[] decisionRuleParams = {decisionName, INPUT_DROOLS_RESULT, salience, functionName, weight, INPUT_EXTENDER};
        String decisionRule = new DroolsDecisionRule().render(decisionRuleParams);

        return NEXT_TWO_LINE + (isCustomize ? customizeCode + System.lineSeparator() : "") +
                NEXT_ONE_LINE + decisionRule;
    }

    /**
     * 构造组件规则
     */
    private String buildSingleGroupRule(RtdDroolsElementsGroup group) {
        this.isValidGroupNameAndElements(group);
        DroolsGroupRule groupRule = new DroolsGroupRule();

        String groupName = group.getGroupName();
        DroolsGeneratorUtil.isValidGroupSalience(group.getSalience());
        String salience = Integer.toString(group.getSalience());

        String algorithmName = group.getAlgorithm();
        DroolsGeneratorUtil.isValidAlgorithm(algorithmName);

        String customizeCode = Base64Util.decode(group.getCustomizeCode());
        DroolsGeneratorUtil.isValidCustomizeCode(customizeCode, algorithmName);

        boolean isCustomize = AlgorithmType.CUSTOMIZE.name().equalsIgnoreCase(algorithmName);
        String functionName = isCustomize ?
                DroolsGeneratorUtil.getCustomizeFunctionName(customizeCode) : algorithmName.toLowerCase(Locale.ROOT);
        this.hasDuplicateFunctionName(customizeFunctionNames, isCustomize, functionName);

        String weight = isCustomize ? null : Double.toString(group.getWeight());
        String[] groupRuleParams = {INPUT_DECISION_MAP, GLOBAL_META_DATA, groupName, INPUT_DROOLS_RESULT,
                salience, functionName, weight, INPUT_EXTENDER};
        StringBuilder sb = new StringBuilder();
        sb.append(NEXT_TWO_LINE).append(isCustomize ? customizeCode + "\n" : "");
        sb.append(NEXT_ONE_LINE).append(groupRule.render(groupRuleParams));

        return sb.toString();
    }

    /**
     * 构造内置函数的函数体
     *
     * @return InnerFunction函数体
     */
    private StringBuilder buildInnerFunction() {
        StringBuilder sb = new StringBuilder();
        Arrays.asList(AlgorithmType.values())
                .stream()
                .filter(algorithm -> !algorithm.name().equalsIgnoreCase(AlgorithmType.CUSTOMIZE.name()))
                .forEach(algorithm -> {
                    String functionName = algorithm.name().substring(0, 1)
                            + algorithm.name().substring(1).toLowerCase(Locale.ROOT);
                    DroolsInnerFunction functionBuilder = new DroolsInnerFunction(functionName);
                    sb.append(NEXT_TWO_LINE).append(functionBuilder.render(new String[0]));
                });

        Arrays.asList(DefinedFunctionType.values())
                .stream()
                .forEach(functionName -> {
                    String upperFunctionName = functionName.value().substring(0, 1).toUpperCase(Locale.ROOT) + functionName.value().substring(1);
                    DroolsInnerFunction functionBuilder = new DroolsInnerFunction(upperFunctionName);
                    sb.append(NEXT_TWO_LINE).append(functionBuilder.render(new String[]{INPUT_DROOLS_RESULT, INPUT_START_TIME, INPUT_TIMEOUT}));
                });

        return sb;
    }

    /**
     * 校验是否是合法的droolsDecision对象
     *
     * @param droolsDecision droolsDecision对象
     * @return 校验是否成功
     */
    private boolean isValidDroolsDecision(RtdDroolsDecision droolsDecision) {
        List<RtdDroolsElementsGroup> elementGroups = droolsDecision.getElementsGroups();
        if (droolsDecision == null || elementGroups == null || elementGroups.size() == 0) {
            throw new DroolsGeneratorException("drools decision can no null");
        }

        Set<String> groupNameSet = new HashSet<>();
        elementGroups.stream().forEach(group -> groupNameSet.add(group.getGroupName()));
        if (groupNameSet.size() != elementGroups.size()) {
            throw new DroolsGeneratorException("Has duplicate group name");
        }

        // 非权重算法，限制不能设置权重
        if (!AlgorithmType.WEIGHTING.name().equals(droolsDecision.getAlgorithm())) {
            if (elementGroups.stream().filter(group -> group.getWeight() != 1.0).count() != 0) {
                throw new DroolsGeneratorException("Algorithm is not 'weighting', can not set weight");
            }
        }

        return true;
    }

    /**
     * group元素合法性校验
     */
    private boolean isValidGroupNameAndElements(RtdDroolsElementsGroup group) {
        if (StringUtils.isBlank(group.getGroupName())) {
            throw new DroolsGeneratorException("Group name can not null ....");
        }

        Set<RtdDroolsElement> groupElements = group.getElements();
        if (groupElements == null || groupElements.size() == 0) {
            throw new DroolsGeneratorException("Element can not null ....");
        }
        groupElements.forEach(element -> {
            if (StringUtils.isBlank(element.getName())) {
                throw new DroolsGeneratorException("Element name can not null ....");
            }
        });
        // 非权重算法，限制不能设置权重
        if (!AlgorithmType.WEIGHTING.name().equals(group.getAlgorithm())) {
            if (group.getElements().stream().filter(element -> element.getWeight() != 1.0).count() != 0) {
                throw new DroolsGeneratorException("Algorithm is not 'weighting', can not set weight");
            }
        }
        return true;
    }

    /**
     * 判断自定义函数，是否包含同名
     */
    private boolean hasDuplicateFunctionName(Set<String> functionNames, boolean isCustomize, String functionName) {
        if (isCustomize && functionNames.contains(functionName)) {
            throw new DroolsGeneratorException("Duplicate customize function name ....");
        }
        functionNames.add(functionName);
        return false;
    }

    /**
     * 获取.drl文件的文件名
     */
    private String getDrlFileName(int elementGroupNumber) {
        int index = elementGroupNumber / RTD_DROOLS_MAX_GROUP_PER_FILE;
        return RTD_DROOLS_FILE_PREFIX + (index == 0 ? 1 : index) + RTD_DROOLS_FILE_SUFFIX;
    }
}
