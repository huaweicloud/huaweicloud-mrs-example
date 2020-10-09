package com.huawei.graphbase.rest.request;

import java.util.List;

public class EdgeSearchRspObj {

    private Long totalHits;

    private List<EdgeElement> edgeList;

    public Long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(Long totalHits) {
        this.totalHits = totalHits;
    }

    public List<EdgeElement> getEdgeList() {
        return edgeList;
    }

    public void setEdgeList(List<EdgeElement> edgeList) {
        this.edgeList = edgeList;
    }
}
