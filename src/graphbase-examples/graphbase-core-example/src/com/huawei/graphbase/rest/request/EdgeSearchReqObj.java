package com.huawei.graphbase.rest.request;

import java.util.List;

public class EdgeSearchReqObj {

    private List<PropertyFilter> filterList;

    private String edgeLabel;

    private List<PropertyKeySort> propertyKeySortList;

    private int limit;

    public List<PropertyFilter> getFilterList() {
        return filterList;
    }

    public void setFilterList(List<PropertyFilter> filterList) {
        this.filterList = filterList;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public void setEdgeLabel(String edgeLabel) {
        this.edgeLabel = edgeLabel;
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
