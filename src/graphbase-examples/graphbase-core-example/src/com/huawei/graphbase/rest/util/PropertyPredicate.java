package com.huawei.graphbase.rest.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum PropertyPredicate {
    /**
     * 包含条件
     */
    CONTAINS_IN("contains_in"),

    /**
     * 不包含条件
     */
    CONTAINS_NOT_IN("contains_not_in"),

    /**
     * 文本包含
     */
    TEXT_CONTAINS("text_contains"),

    /**
     * 文本前缀包含
     */
    TEXT_CONTAINS_PREFIX("text_contains_prefix"),

    /**
     * 文本正则包含
     */
    TEXT_CONTAINS_REGEX("text_contains_regex"),

    /**
     * 字符串包含
     */
    STRING_CONTAINS_PREFIX("string_contains_prefix"),

    /**
     * 字符串前缀不包含
     */
    STRING_NOT_CONTAINS_PREFIX("string_not_contains_prefix"),

    /**
     * 字符串正则包含
     */
    STRING_CONTAINS_REGES("string_contains_regex"),

    /**
     * 等值
     */
    EQUAL("="),

    /**
     * 不等
     */
    NOT_EQUAL("!="),

    /**
     * 数值小于
     */
    LESS_THAN("<"),

    /**
     * 数值小于等于
     */
    LESS_THAN_EQUAL("<="),

    /**
     * 数值大于
     */
    GREATER_THAN(">"),

    /**
     * 数值大于等于
     */
    GREATER_THAN_EQUAL(">="),

    /**
     * 数值范围
     */
    RANGE("range"),

    /**
     * 地理位置内部
     */
    GEO_WITHIN("geowithin"),

    /**
     * 地理位置交集
     */
    GEO_INTERSECT("geointersect"),

    /**
     * 地理位置差集
     */
    GEO_DISJOINT("geodisjoint"),

    /**
     * 地理位置包含
     */
    GEO_CONTAINS("geocontains");

    private String value;

    PropertyPredicate(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return this.value;
    }

    /**
     * 条件转化
     *
     * @param value 条件
     * @return 转化条件
     */
    @JsonCreator
    public static PropertyPredicate getEnum(String value) {
        for (PropertyPredicate item : values()) {
            if (item.getValue().equals(value)) {
                return item;
            }
        }
        return null;
    }
}
