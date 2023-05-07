package com.huawei.graphbase.rest.request;

import java.util.List;

public class VertexQueryRspObj {
    private String id;

    private String vertexLabel;

    private List<PropertyRspObj> propertyList;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getVertexLabel() {
        return vertexLabel;
    }

    public void setVertexLabel(String vertexLabel) {
        this.vertexLabel = vertexLabel;
    }

    public List<PropertyRspObj> getPropertyList() {
        return propertyList;
    }

    public void setPropertyList(List<PropertyRspObj> propertyList) {
        this.propertyList = propertyList;
    }

    @Override
    public String toString() {
        return "VertexQueryRspObj{" + "id='" + id + '\'' + ", vertexLabel='" + vertexLabel + '\'' + ", propertyList="
            + propertyList + '}';
    }
}
