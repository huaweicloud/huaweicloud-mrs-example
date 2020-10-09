package com.huawei.graphbase.rest.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VertexLabel {

    public String name;

    // 暂未使用
    @JsonProperty("isPartitioned")
    public boolean isPartitioned;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonIgnore
    public boolean isPartitioned() {
        return isPartitioned;
    }

    public void setPartitioned(boolean partitioned) {
        isPartitioned = partitioned;
    }

    @Override
    public String toString() {
        return "VertexLabel{" + "name='" + name + '\'' + ", isPartitioned=" + isPartitioned + '}';
    }
}