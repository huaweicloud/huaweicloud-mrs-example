package com.huawei.graphbase.rest.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum DataType {

    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    String,
    Geoshape,
    Date;

    @JsonCreator
    public static DataType getEnum(String name) {
        for (DataType item : values()) {
            if (item.name().equals(name)) {
                return item;
            }
        }
        return null;
    }

    @JsonValue
    public String getName() {
        return name();
    }
}
