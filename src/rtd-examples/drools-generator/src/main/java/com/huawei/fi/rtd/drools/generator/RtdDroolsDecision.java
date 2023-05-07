package com.huawei.fi.rtd.drools.generator;

import java.util.ArrayList;
import java.util.List;

public class RtdDroolsDecision {

    private String decisionName = "drools_decision_9999";

    private String algorithm;

    private int salience = 0;

    private double weight = 1.0;

    // encode by base64
    private String customizeCode;

    private List<RtdDroolsElementsGroup> elementsGroups = new ArrayList<>();

    public RtdDroolsDecision(String algorithm) {

        this.algorithm= algorithm;
    }

    RtdDroolsDecision() {

    }

    public String getDecisionName() {
        return decisionName;
    }

    public int getSalience() {
        return salience;
    }

    public void setSalience(int salience) {
        this.salience = salience;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public String getCustomizeCode() {
        return customizeCode;
    }

    public void setCustomizeCode(String customizeCode) {
        this.customizeCode = customizeCode;
    }

    public void addElementsGroup(RtdDroolsElementsGroup elementsGroup) {
        this.elementsGroups.add(elementsGroup);
    }

    public List<RtdDroolsElementsGroup> getElementsGroups() {
        return this.elementsGroups;
    }
}
