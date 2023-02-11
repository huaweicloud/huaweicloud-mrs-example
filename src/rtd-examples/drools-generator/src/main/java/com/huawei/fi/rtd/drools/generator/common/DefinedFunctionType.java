package com.huawei.fi.rtd.drools.generator.common;

public enum  DefinedFunctionType {

    IS_TIMEOUT("isTimeout");

    private String value = "";

    private DefinedFunctionType(String value)
    {
        this.value = value;
    }

    public String value()
    {
        return this.value;
    }

}
