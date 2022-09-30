package com.huawei.graphbase.rest.entity;

import com.huawei.graphbase.rest.request.PropertyKeyStatus;
import com.huawei.graphbase.rest.util.ElementCategory;
import com.huawei.graphbase.rest.util.IndexType;

import java.util.List;

public class Index {

    private String name;

    private IndexType type;

    private ElementCategory elementCategory;

    private List<PropertyKeyStatus> propertyKeyStatusList;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public IndexType getType() {
        return type;
    }

    public void setType(IndexType type) {
        this.type = type;
    }

    public ElementCategory getElementCategory() {
        return elementCategory;
    }

    public void setElementCategory(ElementCategory elementCategory) {
        this.elementCategory = elementCategory;
    }

    public List<PropertyKeyStatus> getPropertyKeyStatusList() {
        return propertyKeyStatusList;
    }

    public void setPropertyKeyStatusList(List<PropertyKeyStatus> propertyKeyStatusList) {
        this.propertyKeyStatusList = propertyKeyStatusList;
    }

    @Override
    public String toString() {
        return "Index{" + "name='" + name + '\'' + ", type=" + type + ", elementCategory=" + elementCategory
            + ", propertyKeyStatusList=" + propertyKeyStatusList + '}';
    }
}
