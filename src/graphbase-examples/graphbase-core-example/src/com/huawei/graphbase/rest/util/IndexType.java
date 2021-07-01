package com.huawei.graphbase.rest.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum IndexType {
    COMPOSITE,
    MIXED;

    @JsonCreator
    public static IndexType getEnum(String name) {
        for (IndexType item : values()) {
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
