package com.huawei.fi.rtd.drools.generator;

public class RtdDroolsElement {

    private String name;

    private double weight = 1.0d;

    private double missingValue = 0.0d;

    public RtdDroolsElement() {

    }

    public RtdDroolsElement(String name) {
        this.name = name;
    }

    public RtdDroolsElement(String name, double weight) {
        this.name = name;
        this.weight = weight;
    }

    public RtdDroolsElement(String name, double weight, double missingValue) {
        this.name = name;
        this.weight = weight;
        this.missingValue = missingValue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public double getMissingValue() {
        return missingValue;
    }

    public void setMissingValue(double missingValue) {
        this.missingValue = missingValue;
    }
}
