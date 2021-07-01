package com.huawei.graphbase.rest.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum Cardinality {
    SINGLE,
    LIST,
    SET;

    @JsonCreator
    public static Cardinality getEnum(String name) {
        for (Cardinality item : values()) {
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
