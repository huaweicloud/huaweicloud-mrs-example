package com.huawei.graphbase.rest.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ElementCategory {
    VERTEX,
    EDGE;

    @JsonCreator
    public static ElementCategory getEnum(String name) {
        for (ElementCategory item : values()) {
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

