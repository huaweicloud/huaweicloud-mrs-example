package com.huawei.bigdata.spark.examples.datasources;

public class HBaseRecord {
    private String key;
    private Boolean col1;
    private Double col2;
    private Float col3;
    private Integer col4;
    private Long col5;
    private Short col6;
    private String col7;

    HBaseRecord(int i) {
        String tempStr = String.valueOf(i);
        this.key = tempStr;
        this.col1 = i % 2 == 0;
        this.col2 = Double.parseDouble(tempStr);
        this.col3 = Float.parseFloat(tempStr);
        this.col4 = i;
        this.col5 = Long.parseLong(tempStr);
        this.col6 = Short.parseShort(tempStr);
        this.col7 = "String" + i + " extra";
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Boolean getCol1() {
        return col1;
    }

    public void setCol1(Boolean col1) {
        this.col1 = col1;
    }

    public Double getCol2() {
        return col2;
    }

    public void setCol2(Double col2) {
        this.col2 = col2;
    }

    public Float getCol3() {
        return col3;
    }

    public void setCol3(Float col3) {
        this.col3 = col3;
    }

    public Integer getCol4() {
        return col4;
    }

    public void setCol4(Integer col4) {
        this.col4 = col4;
    }

    public Long getCol5() {
        return col5;
    }

    public void setCol5(Long col5) {
        this.col5 = col5;
    }

    public Short getCol6() {
        return col6;
    }

    public void setCol6(Short col6) {
        this.col6 = col6;
    }

    public String getCol7() {
        return col7;
    }

    public void setCol7(String col7) {
        this.col7 = col7;
    }
}
