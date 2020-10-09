package com.huawei.graphbase.rest.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum IkAnalyzer {

    ik_smart,
    ik_max_word;

    @JsonCreator
    public static IkAnalyzer getEnum(String name) {
        for (IkAnalyzer item : values()) {
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
