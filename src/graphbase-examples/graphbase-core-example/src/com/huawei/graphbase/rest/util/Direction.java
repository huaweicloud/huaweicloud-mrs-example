package com.huawei.graphbase.rest.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum Direction {

    BOTH;

    @JsonCreator
    public static Direction getEnum(String name) {
        for (Direction item : values()) {
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
