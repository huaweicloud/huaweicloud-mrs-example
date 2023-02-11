package com.huawei.fi.rtd.drools.generator.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

public class DroolsDecisionRule extends AbstractTemplate {

    private static final Logger logger = LoggerFactory.getLogger(DroolsDecisionRule.class);

    private ST st;

    public DroolsDecisionRule() {
        super();
        st = stGroup.getInstanceOf("droolsDecisionRuleBody");
    }

    public DroolsDecisionRule(String instanceOf) {
        super();
        st = stGroup.getInstanceOf(instanceOf);
    }

    @Override
    public String render(String... params) {

        st.add("decisionName", params[0]);
        st.add("droolsResult", params[1]);
        st.add("salience", params[2]);
        st.add("functionName", params[3]);
        st.add("weight", params[4]);
        st.add("extender", params[5]);

        return st.render();
    }
}