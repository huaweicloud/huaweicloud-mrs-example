package com.huawei.graphbase.rest.request;

import java.util.List;

public class VertexSearchReqObj {

    private List<PropertyFilter> filterList;

    private String vertexLabel;

    private List<PropertyKeySort> propertyKeySortList;

    private int limit;

    public List<PropertyFilter> getFilterList() {
        return filterList;
    }

    public void setFilterList(List<PropertyFilter> filterList) {
        this.filterList = filterList;
    }

    public String getVertexLabel() {
        return vertexLabel;
    }

    public void setVertexLabel(String vertexLabel) {
        this.vertexLabel = vertexLabel;
    }

    public List<PropertyKeySort> getPropertyKeySortList() {
        return propertyKeySortList;
    }

    public void setPropertyKeySortList(List<PropertyKeySort> propertyKeySortList) {
        this.propertyKeySortList = propertyKeySortList;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }
}
