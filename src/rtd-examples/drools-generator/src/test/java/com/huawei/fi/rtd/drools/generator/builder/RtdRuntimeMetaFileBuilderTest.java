package com.huawei.fi.rtd.drools.generator.builder;

import com.huawei.fi.rtd.drools.generator.RtdDroolsDecision;
import com.huawei.fi.rtd.drools.generator.RtdDroolsElement;
import com.huawei.fi.rtd.drools.generator.RtdDroolsElementsGroup;
import com.huawei.fi.rtd.drools.generator.common.AlgorithmType;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RtdRuntimeMetaFileBuilderTest {

    private static final Logger logger = LoggerFactory.getLogger(RtdRuntimeMetaFileBuilderTest.class);

    @Test
    public void gen_rtd_decision_object_output_success() {

        RtdDroolsDecision droolsDecision = new RtdDroolsDecision(AlgorithmType.CUSTOMIZE.name());

        RtdDroolsElementsGroup weightGroup = new RtdDroolsElementsGroup("drools_group_01", AlgorithmType.WEIGHTING.name());
        weightGroup.setSalience(99);
        weightGroup.setWeight(0.5);
        weightGroup.addElement(new RtdDroolsElement("pr_rule_01"));
        weightGroup.addElement(new RtdDroolsElement("pr_rule_02"));

        RtdDroolsElementsGroup customizeGroup = new RtdDroolsElementsGroup("drools_group_02", AlgorithmType.CUSTOMIZE.name());
        customizeGroup.setSalience(99);
        customizeGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        customizeGroup.addElement(new RtdDroolsElement("pr_rule_03"));

        RtdDroolsElementsGroup averageGroup = new RtdDroolsElementsGroup("drools_group_03", AlgorithmType.AVERAGE.name());
        averageGroup.setSalience(99);
        averageGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        averageGroup.addElement(new RtdDroolsElement("pr_rule_03"));
        averageGroup.addElement(new RtdDroolsElement("pr_rule_04"));

        RtdDroolsElementsGroup maximumGroup = new RtdDroolsElementsGroup("drools_group_04", AlgorithmType.MAXIMUM.name());
        maximumGroup.setSalience(99);
        maximumGroup.addElement(new RtdDroolsElement("pr_rule_01"));
        maximumGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        maximumGroup.addElement(new RtdDroolsElement("pr_rule_03"));

        RtdDroolsElementsGroup minimumGroup = new RtdDroolsElementsGroup("drools_group_05", AlgorithmType.MINIMUM.name());
        minimumGroup.setSalience(99);
        minimumGroup.addElement(new RtdDroolsElement("pr_rule_01"));
        minimumGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        minimumGroup.addElement(new RtdDroolsElement("pr_rule_03"));

        droolsDecision.addElementsGroup(weightGroup);
        droolsDecision.addElementsGroup(customizeGroup);
        droolsDecision.addElementsGroup(averageGroup);
        droolsDecision.addElementsGroup(maximumGroup);
        droolsDecision.addElementsGroup(minimumGroup);

        RtdRuntimeMetaFileBuilder builder = new RtdRuntimeMetaFileBuilder();
        String jsonString = builder.buildMetaFile(droolsDecision);

        logger.info("\n" + jsonString);
        Assert.assertTrue(StringUtils.isNotBlank(jsonString));
    }
}
