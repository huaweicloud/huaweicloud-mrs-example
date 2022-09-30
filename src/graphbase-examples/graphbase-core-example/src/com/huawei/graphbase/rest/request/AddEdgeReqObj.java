package com.huawei.graphbase.rest.request;

import java.util.List;

public class AddEdgeReqObj {
    private String outVertexId;

    private String inVertexId;

    private String edgeLabel;

    private List<PropertyReqObj> propertyList;

    public String getOutVertexId() {
        return outVertexId;
    }

    public void setOutVertexId(String outVertexId) {
        this.outVertexId = outVertexId;
    }

    public String getInVertexId() {
        return inVertexId;
    }

    public void setInVertexId(String inVertexId) {
        this.inVertexId = inVertexId;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public void setEdgeLabel(String edgeLabel) {
        this.edgeLabel = edgeLabel;
    }

    public List<PropertyReqObj> getPropertyList() {
        return propertyList;
    }

    public void setPropertyList(List<PropertyReqObj> propertyList) {
        this.propertyList = propertyList;
    }

    @Override
    public String toString() {
        return "AddEdgeReqObj{" + "outVertexId='" + outVertexId + '\'' + ", inVertexId='" + inVertexId + '\''
            + ", edgeLabel='" + edgeLabel + '\'' + ", propertyList=" + propertyList + '}';
    }
}
