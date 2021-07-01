package com.huawei.graphbase.rest.request;

import java.util.List;

public class EdgeElement {

    private String edgeLabel;

    private String edgeId;

    private String outVertexId;

    private String inVertexId;

    private List<PropertyRspObj> propertyList;

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public void setEdgeLabel(String edgeLabel) {
        this.edgeLabel = edgeLabel;
    }

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

    public List<PropertyRspObj> getPropertyList() {
        return propertyList;
    }

    public void setPropertyList(List<PropertyRspObj> propertyList) {
        this.propertyList = propertyList;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public void setEdgeId(String edgeId) {
        this.edgeId = edgeId;
    }
}
