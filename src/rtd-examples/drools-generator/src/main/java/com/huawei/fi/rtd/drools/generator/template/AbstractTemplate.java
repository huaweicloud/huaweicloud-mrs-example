package com.huawei.fi.rtd.drools.generator.template;

import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

public abstract class AbstractTemplate {

    protected STGroup stGroup = null;

    public AbstractTemplate() {
        stGroup = new STGroupFile("templates/rtd_drools.stg");
    }

    public AbstractTemplate(String templateFile, char delimiterStart, char delimiterStop) {
        stGroup = new STGroupFile("templates/" + templateFile, delimiterStart, delimiterStop);
    }

    public abstract String render(String... params);
}
