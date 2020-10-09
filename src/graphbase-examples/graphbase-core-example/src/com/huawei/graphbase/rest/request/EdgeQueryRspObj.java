package com.huawei.graphbase.rest.request;

import java.util.List;

public class EdgeQueryRspObj {
    private String edgeLabel;

    private String id;

    private String outVertexId;

    private String inVertexId;

    private List<PropertyRspObj> propertyList;

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public void setEdgeLabel(String edgeLabel) {
        this.edgeLabel = edgeLabel;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    @Override
    public String toString() {
        return "EdgeQueryRspObj{" + "edgeLabel='" + edgeLabel + '\'' + ", id='" + id + '\'' + ", outVertexId='"
            + outVertexId + '\'' + ", inVertexId='" + inVertexId + '\'' + ", propertyList=" + propertyList + '}';
    }
}
