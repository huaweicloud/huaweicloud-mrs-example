package com.huawei.graphbase.rest.entity;

import com.huawei.graphbase.rest.util.Direction;

import java.util.List;

public class EdgeLabel {

    private String name;

    private String sortOrder;

    private List<String> sortKeyList;

    private Direction direction;

    private String multiplicity;

    private List<String> signatureList;

    private int ttl = 0;

    private List<String> primaryKeys;


    /**
     * get primary keys
     *
     * @return primaryKeys
     */
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    /**
     * set primaryKeys
     *
     * @param primaryKeys primaryKey list
     */
    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(String sortOrder) {
        this.sortOrder = sortOrder;
    }

    public List<String> getSortKeyList() {
        return sortKeyList;
    }

    public void setSortKeyList(List<String> sortKeyList) {
        this.sortKeyList = sortKeyList;
    }

    public List<String> getSignatureList() {
        return signatureList;
    }

    public void setSignatureList(List<String> signatureList) {
        this.signatureList = signatureList;
    }

    public String getMultiplicity() {
        return multiplicity;
    }

    public void setMultiplicity(String multiplicity) {
        this.multiplicity = multiplicity;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "EdgeLabel{" +
                "name='" + name + '\'' +
                ", sortOrder='" + sortOrder + '\'' +
                ", sortKeyList=" + sortKeyList +
                ", direction=" + direction +
                ", multiplicity='" + multiplicity + '\'' +
                ", signatureList=" + signatureList +
                ", ttl=" + ttl +
                ",primaryKeys=" + primaryKeys +
                '}';
    }
}
