package com.huawei.bigdata.flink.examples;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;

@JsonPropertyOrder({"id","name","info"})
public class SomePojo implements Serializable{
    public String id;
    public String name;
    public  String info;

    @Override
    public String toString() {
        return "SomePojo{"
                + "id="
                + id
                + ", name="
                + name
                + ", info="
                + info
                + '}';
    }
}
