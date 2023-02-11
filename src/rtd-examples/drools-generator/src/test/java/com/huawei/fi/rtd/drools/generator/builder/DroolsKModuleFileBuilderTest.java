package com.huawei.fi.rtd.drools.generator.builder;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsKModuleFileBuilderTest {

    private static final Logger logger = LoggerFactory.getLogger(DroolsKModuleFileBuilderTest.class);

    @Test
    public void generate_xml_string_normal_and_success() {

        DroolsKModuleFileBuilder builder = new DroolsKModuleFileBuilder();
        String code = builder.buildXmlFile();

        logger.info("\n" + code);
        Assert.assertTrue(code.contains("name=\"rtdDecisionStateless\"/>"));
    }
}
