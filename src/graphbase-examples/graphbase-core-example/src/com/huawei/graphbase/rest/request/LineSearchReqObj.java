package com.huawei.graphbase.rest.request;

import java.util.List;

public class LineSearchReqObj {
    private List<Long> vertexIdList;

    private int layer;

    private List<VertexFilter> vertexFilterList;

    private List<EdgeFilter> edgeFilterList;

    private Boolean withProperty;

    private Boolean onlyLastLayer;

    private Boolean withCirclePath;

    private Boolean withPath;

    private List<String> vertexPropertyList;

    private Boolean withVertexProperty;

    private int limit = 1000;

    private int vertexEdgeLimit = Integer.MAX_VALUE;

    public Boolean getWithPath() {
        return withPath;
    }

    public void setWithPath(Boolean withPath) {
        this.withPath = withPath;
    }

    public Boolean getOnlyLastLayer() {
        return onlyLastLayer;
    }

    public void setOnlyLastLayer(Boolean onlyLastLayer) {
        this.onlyLastLayer = onlyLastLayer;
    }

    public Boolean getWithCirclePath() {
        return withCirclePath;
    }

    public void setWithCirclePath(Boolean withCirclePath) {
        this.withCirclePath = withCirclePath;
    }

    public List<Long> getVertexIdList() {
        return vertexIdList;
    }

    public void setVertexIdList(List<Long> vertexIdList) {
        this.vertexIdList = vertexIdList;
    }

    public int getLayer() {
        return layer;
    }

    public void setLayer(int layer) {
        this.layer = layer;
    }

    public List<VertexFilter> getVertexFilterList() {
        return vertexFilterList;
    }

    public void setVertexFilterList(List<VertexFilter> vertexFilterList) {
        this.vertexFilterList = vertexFilterList;
    }

    public List<EdgeFilter> getEdgeFilterList() {
        return edgeFilterList;
    }

    public void setEdgeFilterList(List<EdgeFilter> edgeFilterList) {
        this.edgeFilterList = edgeFilterList;
    }

    public Boolean getWithProperty() {
        return withProperty;
    }

    public void setWithProperty(Boolean withProperty) {
        this.withProperty = withProperty;
    }

    public List<String> getVertexPropertyList() {
        return vertexPropertyList;
    }

    public void setVertexPropertyList(List<String> vertexPropertyList) {
        this.vertexPropertyList = vertexPropertyList;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getVertexEdgeLimit() {
        return vertexEdgeLimit;
    }

    public void setVertexEdgeLimit(int vertexEdgeLimit) {
        this.vertexEdgeLimit = vertexEdgeLimit;
    }

    /**
     * 是否返回点属性
     *
     * @return 是否返回点属性
     */
    public Boolean getWithVertexProperty() {
        return withVertexProperty;
    }

    /**
     * 设置是否返回点属性
     *
     * @param withVertexProperty 是否返回点属性
     */
    public void setWithVertexProperty(Boolean withVertexProperty) {
        this.withVertexProperty = withVertexProperty;
    }
}
