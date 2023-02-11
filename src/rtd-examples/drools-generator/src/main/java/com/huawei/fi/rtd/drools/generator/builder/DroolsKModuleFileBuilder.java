package com.huawei.fi.rtd.drools.generator.builder;

import com.huawei.fi.rtd.drools.generator.template.DroolsKModuleXml;

public class DroolsKModuleFileBuilder {

    public String buildXmlFile() {

        DroolsKModuleXml kModuleXml = new DroolsKModuleXml();
        return kModuleXml.render();
    }
}
