package com.huawei.fi.rtd.drools.generator.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

public class DroolsGroupRule extends AbstractTemplate {

    private static final Logger logger = LoggerFactory.getLogger(DroolsGroupRule.class);

    private ST st;

    public DroolsGroupRule() {
        super();
        st = stGroup.getInstanceOf("droolsGroupRuleBody");
    }

    public DroolsGroupRule(String instanceOf) {
        super();
        st = stGroup.getInstanceOf(instanceOf);
    }

    @Override
    public String render(String... params) {

        st.add("decisionMap", params[0]);
        st.add("metaData", params[1]);
        st.add("groupName", params[2]);
        st.add("droolsResult", params[3]);
        st.add("salience", params[4]);
        st.add("functionName", params[5]);
        st.add("weight", params[6]);
        st.add("extender", params[7]);

        return st.render();
    }
}