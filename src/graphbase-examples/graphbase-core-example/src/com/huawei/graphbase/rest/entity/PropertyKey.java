package com.huawei.graphbase.rest.entity;

import com.huawei.graphbase.rest.util.Cardinality;
import com.huawei.graphbase.rest.util.DataType;

public class PropertyKey {

    private String name;

    private DataType dataType;

    private Cardinality cardinality;

    private int ttl = 0;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public Cardinality getCardinality() {
        return cardinality;
    }

    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "PropertyKey{" + "name='" + name + '\'' + ", dataType=" + dataType + ", cardinality=" + cardinality
            + ", ttl=" + ttl + '}';
    }
}
