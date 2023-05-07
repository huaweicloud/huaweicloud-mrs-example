package com.huawei.graphbase.rest.request;

public class PropertyReqObj {

    private String name;

    private Object value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "PropertyReqObj{" + "name='" + name + '\'' + ", value='" + value.toString() + '\'' + '}';
    }
}
