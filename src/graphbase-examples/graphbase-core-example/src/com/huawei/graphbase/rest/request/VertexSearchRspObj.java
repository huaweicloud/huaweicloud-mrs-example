package com.huawei.graphbase.rest.request;

import java.util.List;

public class VertexSearchRspObj {

    private Long totalHits;

    private List<VertexElement> vertexList;

    public Long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(Long totalHits) {
        this.totalHits = totalHits;
    }

    public List<VertexElement> getVertexList() {
        return vertexList;
    }

    public void setVertexList(List<VertexElement> vertexList) {
        this.vertexList = vertexList;
    }
}
