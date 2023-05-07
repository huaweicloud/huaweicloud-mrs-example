package com.huawei.fi.rtd.drools.generator.builder;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.huawei.fi.rtd.drools.generator.RtdDroolsDecision;
import com.huawei.fi.rtd.drools.generator.RtdDroolsElement;
import com.huawei.fi.rtd.drools.generator.RtdDroolsElementsGroup;
import com.huawei.fi.rtd.drools.generator.common.AlgorithmType;
import com.huawei.fi.rtd.drools.generator.common.Base64Util;
import com.huawei.fi.rtd.drools.generator.exception.DroolsGeneratorException;
import com.huawei.fi.rtd.drools.generator.template.DroolsKModuleXml;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.io.KieResources;
import org.kie.api.io.Resource;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DroolsRuntimeFileBuilderTest {

    private static final Logger logger = LoggerFactory.getLogger(DroolsRuntimeFileBuilderTest.class);

    private static final String NEXT_LINE = "\n";

    private KieContainer kieContainer;

    private StatelessKieSession kieSession;

    private RtdDroolsDecision droolsDecision;

    private Map<String, Set<Map<String, Object>>> metaMap;

    private Map<String, Double> decisionMap;

    @Before
    public void setUp() {

        droolsDecision = new RtdDroolsDecision(AlgorithmType.CUSTOMIZE.name());
        droolsDecision.setCustomizeCode(Base64Util.encode(this.genCustomizeCodeFunctionHello()));

        RtdDroolsElementsGroup weightGroup = new RtdDroolsElementsGroup("drools_group_01", AlgorithmType.WEIGHTING.name());
        weightGroup.setSalience(1);
        weightGroup.addElement(new RtdDroolsElement("pr_rule_01", 0.5));
        weightGroup.addElement(new RtdDroolsElement("pr_rule_02", 0.5));

        RtdDroolsElementsGroup customizeGroup = new RtdDroolsElementsGroup("drools_group_02", AlgorithmType.CUSTOMIZE.name());
        customizeGroup.setSalience(2);
        customizeGroup.setCustomizeCode(Base64Util.encode(this.genCustomizeCodeFunctionWorld()));
        customizeGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        customizeGroup.addElement(new RtdDroolsElement("pr_rule_03"));

        RtdDroolsElementsGroup averageGroup = new RtdDroolsElementsGroup("drools_group_03", AlgorithmType.AVERAGE.name());
        averageGroup.setSalience(3);
        averageGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        averageGroup.addElement(new RtdDroolsElement("pr_rule_03"));
        averageGroup.addElement(new RtdDroolsElement("pr_rule_04"));

        RtdDroolsElementsGroup maximumGroup = new RtdDroolsElementsGroup("drools_group_04", AlgorithmType.MAXIMUM.name());
        maximumGroup.setSalience(4);
        maximumGroup.addElement(new RtdDroolsElement("pr_rule_01"));
        maximumGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        maximumGroup.addElement(new RtdDroolsElement("pr_rule_03"));

        RtdDroolsElementsGroup minimumGroup = new RtdDroolsElementsGroup("drools_group_05", AlgorithmType.MINIMUM.name());
        minimumGroup.setSalience(5);
        minimumGroup.addElement(new RtdDroolsElement("pr_rule_01"));
        minimumGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        minimumGroup.addElement(new RtdDroolsElement("pr_rule_03"));

        droolsDecision.addElementsGroup(weightGroup);
        droolsDecision.addElementsGroup(customizeGroup);
        droolsDecision.addElementsGroup(averageGroup);
        droolsDecision.addElementsGroup(maximumGroup);
        droolsDecision.addElementsGroup(minimumGroup);

        metaMap = new HashMap<>();
        Set<Map<String, Object>> metaSet1 = new HashSet<>();
        Map<String, Object> set1_element_01 = new HashMap<>();
        set1_element_01.put("name", "pr_rule_01");
        set1_element_01.put("weight", 0.5);
        set1_element_01.put("missingValue", 0.0);
        metaSet1.add(set1_element_01);

        Map<String, Object> set1_element_02 = new HashMap<>();
        set1_element_02.put("name", "pr_rule_02");
        set1_element_02.put("weight", 0.5);
        set1_element_02.put("missingValue", 0.0);
        metaSet1.add(set1_element_02);
        metaMap.put("drools_group_01", metaSet1);




        Set<Map<String, Object>> metaSet2 = new HashSet<>();
        Map<String, Object> set2_element_02 = new HashMap<>();
        set2_element_02.put("name", "pr_rule_02");
        set2_element_02.put("weight", 1.0);
        set2_element_02.put("missingValue", 0.0);
        metaSet2.add(set2_element_02);

        Map<String, Object> set2_element_03 = new HashMap<>();
        set2_element_03.put("name", "pr_rule_03");
        set2_element_03.put("weight", 1.0);
        set2_element_03.put("missingValue", 0.0);
        metaSet2.add(set2_element_03);
        metaMap.put("drools_group_02", metaSet2);




        Set<Map<String, Object>> metaSet3 = new HashSet<>();
        Map<String, Object> set3_element_02 = new HashMap<>();
        set3_element_02.put("name", "pr_rule_02");
        set3_element_02.put("weight", 1.0);
        set3_element_02.put("missingValue", 0.0);
        metaSet3.add(set3_element_02);

        Map<String, Object> set3_element_03 = new HashMap<>();
        set3_element_03.put("name", "pr_rule_03");
        set3_element_03.put("weight", 1.0);
        set3_element_03.put("missingValue", 0.0);
        metaSet3.add(set3_element_03);

        Map<String, Object> set3_element_04 = new HashMap<>();
        set3_element_04.put("name", "pr_rule_04");
        set3_element_04.put("weight", 1.0);
        set3_element_04.put("missingValue", 0.0);
        metaSet3.add(set3_element_04);
        metaMap.put("drools_group_03", metaSet3);




        Set<Map<String, Object>> metaSet4 = new HashSet<>();
        Map<String, Object> set4_element_01 = new HashMap<>();
        set4_element_01.put("name", "pr_rule_01");
        set4_element_01.put("weight", 1.0);
        set4_element_01.put("missingValue", 0.0);
        metaSet4.add(set4_element_01);

        Map<String, Object> set4_element_02 = new HashMap<>();
        set4_element_02.put("name", "pr_rule_02");
        set4_element_02.put("weight", 1.0);
        set4_element_02.put("missingValue", 0.0);
        metaSet4.add(set4_element_02);

        Map<String, Object> set4_element_03 = new HashMap<>();
        set4_element_03.put("name", "pr_rule_03");
        set4_element_03.put("weight", 1.0);
        set4_element_03.put("missingValue", 0.0);
        metaSet4.add(set4_element_03);
        metaMap.put("drools_group_04", metaSet4);




        Set<Map<String, Object>> metaSet5 = new HashSet<>();
        Map<String, Object> set5_element_01 = new HashMap<>();
        set5_element_01.put("name", "pr_rule_01");
        set5_element_01.put("weight", 1.0);
        set5_element_01.put("missingValue", 0.0);
        metaSet5.add(set5_element_01);

        Map<String, Object> set5_element_02 = new HashMap<>();
        set5_element_02.put("name", "pr_rule_02");
        set5_element_02.put("weight", 1.0);
        set5_element_02.put("missingValue", 0.0);
        metaSet5.add(set5_element_02);

        Map<String, Object> set5_element_03 = new HashMap<>();
        set5_element_03.put("name", "pr_rule_03");
        set5_element_03.put("weight", 1.0);
        set5_element_03.put("missingValue", 0.0);
        metaSet5.add(set5_element_03);
        metaMap.put("drools_group_05", metaSet5);

        decisionMap = new HashMap<>();
        decisionMap.put("pr_rule_01", 100.4);
        decisionMap.put("pr_rule_02", 150.0);
        decisionMap.put("pr_rule_03", 200.0);
        decisionMap.put("pr_rule_04", 240.0);
    }

    @After
    public void tearDown() {

        if (kieContainer != null) {
            kieContainer.dispose();
        }

        kieContainer = null;
        kieSession = null;
    }

    @Test(expected = DroolsGeneratorException.class)
    public void set_invalid_element_group_salience_and_gen_drl_string_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.getElementsGroups().forEach(group -> {
            if (group.getGroupName().equals("drools_group_01")) {
                group.setSalience(1000);
            }
        });

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void set_algorithm_type_with_avg_but_also_set_customize_code_then_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.setAlgorithm(AlgorithmType.AVERAGE.name());

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void set_algorithm_type_with_customize_but_set_code_with_null_then_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.setAlgorithm(AlgorithmType.CUSTOMIZE.name());
        droolsDecision.setCustomizeCode("");

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void set_invalid_element_group_algorithm_and_gen_drl_string_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.getElementsGroups().forEach(group -> {
            if (group.getGroupName().equals("drools_group_01")) {
                group.setAlgorithm("hello, everyone");
            }
        });

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void element_group_rule_set_is_empty_and_gen_drl_string_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.getElementsGroups().forEach(group -> {
            if (group.getGroupName().equals("drools_group_01")) {
                group.getElements().clear();
            }
        });

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void element_group_is_empty_and_gen_drl_string_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.getElementsGroups().clear();

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void set_invalid_decision_salience_and_gen_drl_string_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.setSalience(100);
        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void set_invalid_algorithm_salience_and_gen_drl_string_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.setAlgorithm("hello, everyone");
        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void set_duplicate_group_name_and_gen_drl_then_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.getElementsGroups().forEach(group -> {
            if (group.getGroupName().equals("drools_group_01")) {
                group.setGroupName("drools_group_02");
            }
        });

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void set_group_weight_but_algorithm_is_not_weighting_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.setAlgorithm(AlgorithmType.MAXIMUM.name());
        droolsDecision.setCustomizeCode("");
        droolsDecision.getElementsGroups().forEach(group -> {
            if (group.getGroupName().equals("drools_group_01")) {
                group.setWeight(0.7);
            }
        });

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void set_element_weight_but_algorithm_is_not_weighting_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.getElementsGroups().forEach(group -> {
            if (group.getGroupName().equals("drools_group_03")) {
                group.getElements().forEach(element -> element.setWeight(0.3));
            }
        });

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void customize_code_define_duplicate_function_name_in_element_group_then_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.getElementsGroups().forEach(group -> {
            if (group.getGroupName().equals("drools_group_01")) {
                group.setAlgorithm(AlgorithmType.CUSTOMIZE.name());
                group.setCustomizeCode(Base64Util.encode(this.genCustomizeCodeFunctionWorld()));
            }
        });

        builder.buildDrlFile(droolsDecision);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void customize_code_define_duplicate_function_name_between_element_group_and_decision_then_throw_exception() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        droolsDecision.setCustomizeCode(Base64Util.encode(this.genCustomizeCodeFunctionWorld()));

        builder.buildDrlFile(droolsDecision);
    }

    @Test
    public void input_valid_drools_decision_contains_customize_code_then_output_drl_success() {

        logger.info("\n" + JSONObject.toJSONString(droolsDecision, SerializerFeature.PrettyFormat));

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        Map<String, String> codeMap = builder.buildDrlFile(droolsDecision);
        logger.info("\n" + codeMap);
        StringBuilder code = new StringBuilder();
        codeMap.values().forEach(c -> code.append(c));

        Assert.assertTrue(code.toString().contains("function double hello("));
        Assert.assertTrue(code.toString().contains("function double world("));
        Assert.assertTrue(code.toString().contains("rule \"drools_decision_9999\""));

        this.initKieContainerAndSession(codeMap);

        Map<String, Object> droolsResult = new HashMap<>();
        Map<String, Object> elementsGroups = new HashMap<>();
        StringBuffer extender = new StringBuffer();
        droolsResult.put("elementsGroups", elementsGroups);

        kieSession.setGlobal("metaData", metaMap);

        Map<String, Object> factMap = new HashMap<>();
        factMap.put("decisionMap", decisionMap);
        factMap.put("droolsResult", droolsResult);
        factMap.put("extender", extender);

        kieSession.execute(factMap);
        logger.info("droolsResult: " + droolsResult);
        logger.info("extender: " + extender.toString());

        Assert.assertTrue(extender.toString().equals("print world@@print hello@@"));

        long droolsScore = (Long) droolsResult.get("droolsScore");
        Assert.assertTrue(droolsScore == 100);

        double group_01 = (Double) ((Map) droolsResult.get("elementsGroups")).get("drools_group_01");
        Assert.assertTrue(group_01 == 125.2);

        double group_02 = (Double) ((Map) droolsResult.get("elementsGroups")).get("drools_group_02");
        Assert.assertTrue(group_02 == 200.0);

        double group_03 = (Double) ((Map) droolsResult.get("elementsGroups")).get("drools_group_03");
        Assert.assertTrue(group_03 - 196.6 < 0.1);

        double group_04 = (Double) ((Map) droolsResult.get("elementsGroups")).get("drools_group_04");
        Assert.assertTrue(group_04 == 200.0);

        double group_05 = (Double) ((Map) droolsResult.get("elementsGroups")).get("drools_group_05");
        Assert.assertTrue(group_05 == 100.4);
    }

    @Test
    public void input_part_of_rule_score_in_element_groups_but_avg_algorithm_also_divided_all_rule_number() {

        decisionMap.remove("pr_rule_02");
        decisionMap.remove("pr_rule_03");

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        Map<String, String> codeMap = builder.buildDrlFile(droolsDecision);

        this.initKieContainerAndSession(codeMap);

        Map<String, Object> droolsResult = new HashMap<>();
        Map<String, Object> elementsGroups = new HashMap<>();
        StringBuffer extender = new StringBuffer();
        droolsResult.put("elementsGroups", elementsGroups);

        kieSession.setGlobal("metaData", metaMap);

        Map<String, Object> factMap = new HashMap<>();
        factMap.put("decisionMap", decisionMap);
        factMap.put("droolsResult", droolsResult);
        factMap.put("extender", extender);

        kieSession.execute(factMap);

        double group_03 = (Double) ((Map) droolsResult.get("elementsGroups")).get("drools_group_03");
        Assert.assertTrue(group_03 == 80);
    }

    @Test
    public void set_different_salience_with_two_appender_string_then_output_extender_sequence() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();

        droolsDecision.setAlgorithm(AlgorithmType.AVERAGE.name());
        droolsDecision.setCustomizeCode("");
        droolsDecision.getElementsGroups().forEach(group -> {
            if (group.getGroupName().equals("drools_group_01")) {
                group.setAlgorithm(AlgorithmType.CUSTOMIZE.name());
                group.setCustomizeCode(Base64Util.encode(this.genCustomizeCodeFunctionHello()));
                group.getElements().forEach(element -> element.setWeight(1.0));
                group.setSalience(999);
            }
        });

        Map<String, String> codeMap = builder.buildDrlFile(droolsDecision);
        this.initKieContainerAndSession(codeMap);

        Map<String, Object> droolsResult = new HashMap<>();
        Map<String, Object> elementsGroups = new HashMap<>();
        StringBuffer extender = new StringBuffer();
        droolsResult.put("elementsGroups", elementsGroups);

        kieSession.setGlobal("metaData", metaMap);

        Map<String, Object> factMap = new HashMap<>();
        factMap.put("decisionMap", decisionMap);
        factMap.put("droolsResult", droolsResult);
        factMap.put("extender", extender);

        kieSession.execute(factMap);
        logger.info("droolsResult: " + droolsResult);
        logger.info("extender: " + extender.toString());

        Assert.assertTrue(extender.toString().equals("print hello@@print world@@"));
    }

    @Test
    public void input_timeout_value_and_score_normal_then_out_timeout_exception_information() {

        DroolsRuntimeFileBuilder builder = new DroolsRuntimeFileBuilder();
        Map<String, String> codeMap = builder.buildDrlFile(droolsDecision);

        this.initKieContainerAndSession(codeMap);

        Map<String, Object> droolsResult = new HashMap<>();
        Map<String, Object> elementsGroups = new HashMap<>();
        StringBuffer extender = new StringBuffer();
        droolsResult.put("elementsGroups", elementsGroups);

        kieSession.setGlobal("metaData", metaMap);

        Map<String, Object> factMap = new HashMap<>();
        factMap.put("decisionMap", decisionMap);
        factMap.put("droolsResult", droolsResult);
        factMap.put("extender", extender);
        factMap.put("timeout", 1L);
        factMap.put("startTime", System.currentTimeMillis() - 1000);

        kieSession.execute(factMap);

        String exception = (String) droolsResult.get("exception");
        Assert.assertTrue("timeout".equals(exception));
    }

    private String genCustomizeCodeFunctionHello() {

        StringBuilder sb = new StringBuilder();
        sb.append("function double hello(Map computeResults, Set metaDataSet, StringBuffer extender) {").append(NEXT_LINE).append(NEXT_LINE);

        sb.append("    double resultScore = 0.0;").append(NEXT_LINE);
        sb.append("    boolean isFirstElement = true;").append(NEXT_LINE);
        sb.append("    for(Object metaData : metaDataSet) {").append(NEXT_LINE);
        sb.append("        String metaName = (String)((Map) metaData).get(\"name\");").append(NEXT_LINE);
        sb.append("        double metaWeight = (Double)((Map) metaData).get(\"weight\");").append(NEXT_LINE);
        sb.append("        double metaMissingValue = (Double)((Map) metaData).get(\"missingValue\");").append(NEXT_LINE);
        sb.append("        Object computedScore = computeResults.get(metaName);").append(NEXT_LINE);
        sb.append("        computedScore = computedScore == null ? metaMissingValue : Double.parseDouble(computedScore.toString());").append(NEXT_LINE);
        sb.append("        double finalComputedScore = (Double) computedScore * metaWeight;").append(NEXT_LINE);
        sb.append("        if(isFirstElement) {").append(NEXT_LINE);
        sb.append("            isFirstElement = false;").append(NEXT_LINE);
        sb.append("            resultScore = finalComputedScore;").append(NEXT_LINE);
        sb.append("        }").append(NEXT_LINE);
        sb.append("        resultScore = Math.min(resultScore, finalComputedScore);").append(NEXT_LINE);
        sb.append("    }").append(NEXT_LINE).append(NEXT_LINE);

        sb.append("    extender.append(\"print hello@@\");").append(NEXT_LINE);
        sb.append("    return resultScore;").append(NEXT_LINE);
        sb.append("}");

        return sb.toString();
    }

    private String genCustomizeCodeFunctionWorld() {

        StringBuilder sb = new StringBuilder();
        sb.append("function double world(Map computeResults, Set metaDataSet, StringBuffer extender) {").append(NEXT_LINE).append(NEXT_LINE);

        sb.append("    Double resultScore = 0.1;").append(NEXT_LINE);
        sb.append("    boolean isFirstElement = true;").append(NEXT_LINE);
        sb.append("    for(Object metaData : metaDataSet) {").append(NEXT_LINE);
        sb.append("        String metaName = (String)((Map) metaData).get(\"name\");").append(NEXT_LINE);
        sb.append("        double metaWeight = (Double)((Map) metaData).get(\"weight\");").append(NEXT_LINE);
        sb.append("        double metaMissingValue = (Double)((Map) metaData).get(\"missingValue\");").append(NEXT_LINE);
        sb.append("        Object computedScore = computeResults.get(metaName);").append(NEXT_LINE);
        sb.append("        computedScore = computedScore == null ? metaMissingValue : Double.parseDouble(computedScore.toString());").append(NEXT_LINE);
        sb.append("        double finalComputedScore = (Double) computedScore * metaWeight;").append(NEXT_LINE);
        sb.append("        if(isFirstElement) {").append(NEXT_LINE);
        sb.append("            isFirstElement = false;").append(NEXT_LINE);
        sb.append("            resultScore = finalComputedScore;").append(NEXT_LINE);
        sb.append("        }").append(NEXT_LINE);
        sb.append("        resultScore = Math.max(resultScore, finalComputedScore);").append(NEXT_LINE);
        sb.append("    }").append(NEXT_LINE).append(NEXT_LINE);

        sb.append("    extender.append(\"print world@@\");").append(NEXT_LINE);
        sb.append("    return resultScore;").append(NEXT_LINE);
        sb.append("}");

        return sb.toString();
    }

    private void initKieContainerAndSession(Map<String, String> codeMap) {

        KieResources kr= KieServices.Factory.get().getResources();

        KieServices ks = KieServices.Factory.get();
        KieFileSystem kfs = ks.newKieFileSystem();
        kfs.writeKModuleXML(new DroolsKModuleXml().render());

        for (Map.Entry<String, String> entry : codeMap.entrySet()) {
            Resource drlResource = kr.newByteArrayResource(entry.getValue().getBytes()).setResourceType(ResourceType.DRL);
            drlResource.setSourcePath("com/huawei/fi/rtd/drools/common/" + entry.getKey());
            kfs.write(drlResource);
        }

        KieBuilder kb = ks.newKieBuilder(kfs);
        kb.buildAll();

        kieContainer = ks.newKieContainer(ks.getRepository().getDefaultReleaseId());
        kieSession = kieContainer.newStatelessKieSession();
    }

}
