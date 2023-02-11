package com.huawei.fi.rtd.drools.generator.template;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsKModuleXmlTest {

    private static final Logger logger = LoggerFactory.getLogger(DroolsKModuleXmlTest.class);

    @Test
    public void generate_xml_string_normal_and_success() {

        DroolsKModuleXml xml = new DroolsKModuleXml();
        String code = xml.render();

        logger.info("\n" + code);

        Assert.assertTrue(code.contains("name=\"rtdDecisionStateless\"/>"));
    }
}
