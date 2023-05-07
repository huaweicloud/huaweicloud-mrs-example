package com.huawei.fi.rtd.drools.generator.template;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsDecisionRuleTest {

    private static final Logger logger = LoggerFactory.getLogger(DroolsDecisionRuleTest.class);

    @Test
    public void gen_normal_code_input_expect_tag_then_output_success() {

        DroolsDecisionRule rule = new DroolsDecisionRule("droolsDecisionRuleBody");
        String code = rule.render(new String[]{"drools_decision_9999", "droolsResult", "0", "weighting", "1.0", "extender"});

        logger.info("\n" + code);
        Assert.assertTrue(code.startsWith("rule \"drools_decision_9999\""));
    }
}
