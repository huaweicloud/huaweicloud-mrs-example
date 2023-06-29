package com.huawei.fi.rtd.drools.generator.common;

import com.huawei.fi.rtd.drools.generator.exception.DroolsGeneratorException;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;

public class DroolsGeneratorUtilTest {

    private static final String NEXT_LINE = "\n";

    @Test(expected = DroolsGeneratorException.class)
    public void input_invalid_algorithm_and_throw_exception() {

        String algorithmName = "abc";
        DroolsGeneratorUtil.isValidAlgorithm(algorithmName);
    }

    @Test
    public void input_valid_customize_code_and_check_success() {

        boolean result = DroolsGeneratorUtil.isValidCustomizeCode(this.genCustomizeCodeFunctionHello(), AlgorithmType.CUSTOMIZE.name());
        Assert.assertTrue(result);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void input_invalid_customize_code_and_throw_exception() {

        DroolsGeneratorUtil.isValidCustomizeCode("hello", AlgorithmType.CUSTOMIZE.name());
    }

    @Test
    public void input_valid_customize_code_and_get_function_name_then_success() {

        String functionName = DroolsGeneratorUtil.getCustomizeFunctionName(this.genCustomizeCodeFunctionHello());
        Assert.assertTrue("test".equals(functionName));
    }

    @Test(expected = DroolsGeneratorException.class)
    public void input_invalid_group_salience_then_throw_exception() {

        DroolsGeneratorUtil.isValidGroupSalience(10000);
    }

    @Test(expected = DroolsGeneratorException.class)
    public void input_invalid_decision_salience_then_throw_exception() {

        DroolsGeneratorUtil.isValidDecisionSalience(10000);
    }

    @Test
    public void input_not_exists_directory_path_then_return_false() {

        boolean result = DroolsGeneratorUtil.isValidDirectoryPath("d:/hello.sh");
        Assert.assertFalse(result);
    }

    @Test
    public void input_contains_while_loop_code_and_add_timeout_control_code_then_replace_success() {

        String whileLoopCode = "while (true) { while(1==1)}";
        String timeoutControlCode = "isTimeout(factData) && ";

        String result = DroolsGeneratorUtil.addTimeoutControl4WhileLoop(whileLoopCode, timeoutControlCode);
        Assert.assertTrue(result.contains(timeoutControlCode));
    }

    @Test
    public void input_contains_for_loop_code_and_add_timeout_control_code_then_replace_success() {

        String forLoopCode = "for (int i = 0; i<100; i++) { " +
                "               for (int j = 0; j<100; j++) {" +
                "                  System.out.println(\"hello, world\");" +
                "               }" +
                "             }";
        String timeoutControlCode = "isTimeout(factData) && ";

        String result = DroolsGeneratorUtil.addTimeoutControl4ForLoop(forLoopCode, timeoutControlCode);
        Assert.assertTrue(result.contains(timeoutControlCode));
    }

    /**
     * 构造输出一个用户自定义代码
     *
     * @return
     */
    private String genCustomizeCodeFunctionHello() {

        StringBuilder sb = new StringBuilder();
        sb.append("function double test(Map computeResults, Set metaDataSet, StringBuffer extender) {").append(NEXT_LINE).append(NEXT_LINE);

        sb.append("    Double resultScore = 0.0;").append(NEXT_LINE);
        sb.append("    boolean isFirstElement = true;").append(NEXT_LINE);
        sb.append("    for(Object metaData : metaDataSet) {").append(NEXT_LINE);
        sb.append("        Double computedScore = (Double) computeResults.get((String) metaData);").append(NEXT_LINE);
        sb.append("        computedScore = computedScore == null ? 0.0 : computedScore;").append(NEXT_LINE);
        sb.append("        if(isFirstElement) {").append(NEXT_LINE);
        sb.append("            isFirstElement = false;").append(NEXT_LINE);
        sb.append("            resultScore = computedScore;").append(NEXT_LINE);
        sb.append("        }").append(NEXT_LINE);
        sb.append("        resultScore = Math.min(resultScore, computedScore);").append(NEXT_LINE);
        sb.append("    }").append(NEXT_LINE).append(NEXT_LINE);

        sb.append("    return resultScore;").append(NEXT_LINE);
        sb.append("}");

        return sb.toString();
    }
}
