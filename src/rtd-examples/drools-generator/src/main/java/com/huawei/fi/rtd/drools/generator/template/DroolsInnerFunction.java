package com.huawei.fi.rtd.drools.generator.template;

import org.stringtemplate.v4.ST;

public class DroolsInnerFunction extends AbstractTemplate {

    private static final String PREFIX = "droolsFunction";

    private ST st;

    public DroolsInnerFunction(String functionName) {
        super();
        st = stGroup.getInstanceOf(PREFIX + functionName);
    }

    @Override
    public String render(String... params) {

        if (params != null && params.length > 0) {
            st.add("droolsResult", params[0]);
            st.add("startTime", params[1]);
            st.add("timeout", params[2]);
        }

        return st.render();
    }
}
