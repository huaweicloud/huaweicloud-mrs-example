package com.huawei.fi.rtd.drools.generator.template;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsInnerFunctionTest {

    private static final Logger logger = LoggerFactory.getLogger(DroolsInnerFunctionTest.class);

    @Test
    public void gen_normal_code_input_weighting_then_output_success() {

        DroolsInnerFunction function = new DroolsInnerFunction("Weighting");
        String code = function.render(new String[0]);

        logger.info("\n" + code);
        Assert.assertTrue(code.startsWith("function double weighting"));
    }

    @Test
    public void gen_normal_code_input_maximum_then_output_success() {

        DroolsInnerFunction function = new DroolsInnerFunction("Maximum");
        String code = function.render(new String[0]);

        logger.info("\n" + code);
        Assert.assertTrue(code.startsWith("function double maximum"));
    }

    @Test
    public void gen_normal_code_input_minimum_then_output_success() {

        DroolsInnerFunction function = new DroolsInnerFunction("Minimum");
        String code = function.render(new String[0]);

        logger.info("\n" + code);
        Assert.assertTrue(code.startsWith("function double minimum"));
    }

    @Test
    public void gen_normal_code_input_average_then_output_success() {

        DroolsInnerFunction function = new DroolsInnerFunction("Average");
        String code = function.render(new String[0]);

        logger.info("\n" + code);
        Assert.assertTrue(code.startsWith("function double average"));
    }
}
