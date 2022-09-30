package com.huawei.graphbase.rest.request;

import java.util.List;

public class PathSearchReqObj {
    private List<String> vertexIdList;

    private int layer;

    private int limit = 1000;

    private VertexFilter vertexFilter;

    private EdgeFilter edgeFilter;

    //当option = weighted时必选，关系上权值属性名
    private String option;

    //Option不传时，默认为all，则查询全路径；option=shortest，则查询最短路径；option=circle，查询回环路径；option=weighted，查询有权最短路径。
    private String weightedPropertyName;

    private boolean enableShortestCircle;

    private boolean withInnerCircle;

    public boolean isEnableShortestCircle() {
        return enableShortestCircle;
    }

    public void setEnableShortestCircle(boolean enableShortestCircle) {
        this.enableShortestCircle = enableShortestCircle;
    }

    public boolean isWithInnerCircle() {
        return withInnerCircle;
    }

    public void setWithInnerCircle(boolean withInnerCircle) {
        this.withInnerCircle = withInnerCircle;
    }

    public List<String> getVertexIdList() {
        return vertexIdList;
    }

    public void setVertexIdList(List<String> vertexIdList) {
        this.vertexIdList = vertexIdList;
    }

    public int getLayer() {
        return layer;
    }

    public void setLayer(int layer) {
        this.layer = layer;
    }

    public VertexFilter getVertexFilter() {
        return vertexFilter;
    }

    public void setVertexFilter(VertexFilter vertexFilter) {
        this.vertexFilter = vertexFilter;
    }

    public EdgeFilter getEdgeFilter() {
        return edgeFilter;
    }

    public void setEdgeFilter(EdgeFilter edgeFilter) {
        this.edgeFilter = edgeFilter;
    }

    public String getWeightedPropertyName() {
        return weightedPropertyName;
    }

    public void setWeightedPropertyName(String weightedPropertyName) {
        this.weightedPropertyName = weightedPropertyName;
    }

    public String getOption() {
        return option;
    }

    public void setOption(String option) {
        this.option = option;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }
}
