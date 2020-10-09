package com.huawei.graphbase.rest.request;

import java.util.List;

public class AddVertexReqObj {

    private String vertexLabel;

    private String primaryKey;

    private List<PropertyReqObj> propertyList;

    public String getVertexLabel() {
        return vertexLabel;
    }

    public void setVertexLabel(String vertexLabel) {
        this.vertexLabel = vertexLabel;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public List<PropertyReqObj> getPropertyList() {
        return propertyList;
    }

    public void setPropertyList(List<PropertyReqObj> propertyList) {
        this.propertyList = propertyList;
    }

    @Override
    public String toString() {
        return "AddVertexReqObj{" + "vertexLabel='" + vertexLabel + '\'' + ", primaryKey='" + primaryKey + '\''
            + ", propertyList=" + propertyList + '}';
    }
}
