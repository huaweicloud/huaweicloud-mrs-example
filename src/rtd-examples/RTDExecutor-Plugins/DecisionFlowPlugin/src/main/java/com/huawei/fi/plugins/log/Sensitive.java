package com.huawei.fi.plugins.log;

public class Sensitive {

    private String name;
    private short size;
    private short prefixNoMaskLen;
    private short suffixNoMaskLen;
    private String mask = "*";

    public Sensitive(String name) {
        this.name = name;
    }

    public Sensitive(String name, short size, short prefixNoMaskLen, short suffixNoMaskLen) {
        this.name = name;
        this.size = size;
        this.prefixNoMaskLen = prefixNoMaskLen;
        this.suffixNoMaskLen = suffixNoMaskLen;
    }

    public short getSize() {
        return size;
    }

    public void setSize(short size) {
        this.size = size;
    }

    public short getPrefixNoMaskLen() {
        return prefixNoMaskLen;
    }

    public void setPrefixNoMaskLen(short prefixNoMaskLen) {
        this.prefixNoMaskLen = prefixNoMaskLen;
    }

    public short getSuffixNoMaskLen() {
        return suffixNoMaskLen;
    }

    public void setSuffixNoMaskLen(short suffixNoMaskLen) {
        this.suffixNoMaskLen = suffixNoMaskLen;
    }

    public String getName() {
        return name;
    }

    public String getMask() {
        return mask;
    }

    public void setMask(String mask) {
        this.mask = mask;
    }
}