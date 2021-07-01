package com.huawei.graphbase.rest.request;

import java.util.List;

public class VertexElement {

    private String vertexLabel;

    private String id;

    private List<PropertyRspObj> propertyList;

    public String getVertexLabel() {
        return vertexLabel;
    }

    public void setVertexLabel(String vertexLabel) {
        this.vertexLabel = vertexLabel;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<PropertyRspObj> getPropertyList() {
        return propertyList;
    }

    public void setPropertyList(List<PropertyRspObj> propertyList) {
        this.propertyList = propertyList;
    }
}
