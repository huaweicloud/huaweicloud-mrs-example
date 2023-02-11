package com.huawei.fi.rtd.drools.generator;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class RtdDroolsElementsGroup {

    private String groupName;

    private String algorithm;

    private int salience = 11;

    private double weight = 1.0;

    // encode by base64
    private String customizeCode;

    private Set<RtdDroolsElement> elements = new HashSet<>();

    public RtdDroolsElementsGroup(String groupName, String algorithm) {

        this.groupName = groupName;
        this.algorithm = algorithm;
    }

    RtdDroolsElementsGroup() {

    }

    public int getSalience() {
        return salience;
    }

    public void setSalience(int salience) {
        this.salience = salience;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public String getCustomizeCode() {
        return customizeCode;
    }

    public void setCustomizeCode(String customizeCode) {
        this.customizeCode = customizeCode;
    }

    public void addElement(RtdDroolsElement element) {
        this.elements.add(element);
    }

    public Set<RtdDroolsElement> getElements() {
        return this.elements;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RtdDroolsElementsGroup that = (RtdDroolsElementsGroup) o;
        return Objects.equals(groupName, that.groupName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(groupName);
    }
}
