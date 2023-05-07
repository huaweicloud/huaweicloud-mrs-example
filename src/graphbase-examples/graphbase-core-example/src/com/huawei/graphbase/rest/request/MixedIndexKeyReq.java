package com.huawei.graphbase.rest.request;

import com.huawei.graphbase.rest.util.IkAnalyzer;

import java.util.List;

public class MixedIndexKeyReq {

    private String name;

    private List<KeyTextType> keyTextTypeList;

    private IkAnalyzer analyzer;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
