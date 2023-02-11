package com.huawei.fi.rtd.drools.generator.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

public class DroolsFileHeader extends AbstractTemplate {

    private static final Logger logger = LoggerFactory.getLogger(DroolsFileHeader.class);

    private ST st;

    public DroolsFileHeader() {
        super();
        st = stGroup.getInstanceOf("droolsCommonRuleHeader");
    }

    public DroolsFileHeader(String instanceOf) {
        super();
        st = stGroup.getInstanceOf(instanceOf);
    }

    @Override
    public String render(String... params) {

        st.add("metaData", params[0]);

        return st.render();
    }
}
