package com.huawei.fi.rtd.drools.generator.template;

import org.stringtemplate.v4.ST;

public class DroolsKModuleXml extends AbstractTemplate {

    private ST st;

    public DroolsKModuleXml() {
        super("rtd_kmodule.stg", '$', '$');
        st = stGroup.getInstanceOf("droolsKModuleXml");
    }

    @Override
    public String render(String... params) {
        return st.render();
    }

}
