package com.huawei.fi.rtd.drools.generator.template;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsFileHeaderTest {

    private static final Logger logger = LoggerFactory.getLogger(DroolsFileHeaderTest.class);

    @Test
    public void gen_normal_code_input_expect_tag_then_output_success() {

        DroolsFileHeader header = new DroolsFileHeader("droolsCommonRuleHeader");
        String [] params = {"metaData", "droolsResult", "extender"};
        String code = header.render(params);

        logger.info("\n" + code);

        Assert.assertTrue(code.contains("global java.util.HashMap metaData"));
    }
}
