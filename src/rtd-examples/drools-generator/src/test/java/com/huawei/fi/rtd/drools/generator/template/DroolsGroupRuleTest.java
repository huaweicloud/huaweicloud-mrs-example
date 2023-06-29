package com.huawei.fi.rtd.drools.generator.template;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsGroupRuleTest {

    private static final Logger logger = LoggerFactory.getLogger(DroolsGroupRuleTest.class);

    @Test
    public void gen_normal_code_input_expect_tag_then_output_success() {

        DroolsGroupRule rule = new DroolsGroupRule("droolsGroupRuleBody");
        String code = rule.render(new String[]{"decisionMap", "metaData", "elementGroups_01", "droolsResult", "99", "weighting", "1.0", "extender"});

        logger.info("\n" + code);
        Assert.assertTrue(code.startsWith("rule \"elementGroups_01\""));
    }
}
