package com.huawei.graphbase.rest.request;

import com.huawei.graphbase.rest.util.ElementCategory;
import com.huawei.graphbase.rest.util.IkAnalyzer;
import com.huawei.graphbase.rest.util.IndexType;

import java.util.List;

public class GraphIndexReqObj {

    private String name;

    private ElementCategory elementCategory;

    private IndexType Type;

    private boolean unique;

    private List<KeyTextType> keyTextTypeList;

    private IkAnalyzer analyzer;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ElementCategory getElementCategory() {
        return elementCategory;
    }

    public void setElementCategory(ElementCategory elementCategory) {
        this.elementCategory = elementCategory;
    }

    public IndexType getType() {
        return Type;
    }

    public void setType(IndexType type) {
        Type = type;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public List<KeyTextType> getKeyTextTypeList() {
        return keyTextTypeList;
    }

    public void setKeyTextTypeList(List<KeyTextType> keyTextTypeList) {
        this.keyTextTypeList = keyTextTypeList;
    }

    public IkAnalyzer getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(IkAnalyzer analyzer) {
        this.analyzer = analyzer;
    }
}
