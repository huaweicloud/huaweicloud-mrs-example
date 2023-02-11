package com.huawei.graphbase.rest.request;

import com.huawei.graphbase.rest.util.DataType;

import java.util.List;

public class VertexFilter {
    private List<String> vertexLabelList;

    private List<PropertyFilter> filterList;

    private DataType propertyDataType;

    public List<String> getVertexLabelList() {
        return vertexLabelList;
    }

    public void setVertexLabelList(List<String> vertexLabelList) {
        this.vertexLabelList = vertexLabelList;
    }

    public List<PropertyFilter> getFilterList() {
        return filterList;
    }

    public void setFilterList(List<PropertyFilter> filterList) {
        this.filterList = filterList;
    }

    public DataType getPropertyDataType() {
        return propertyDataType;
    }

    public void setPropertyDataType(DataType propertyDataType) {
        this.propertyDataType = propertyDataType;
    }
}
