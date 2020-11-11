package com.huawei.bigdata.spark.examples;

public class SolrException extends Exception {
    static final long serialVersionUID = 0;

    public SolrException() {
        super();
    }

    public SolrException(String message) {
        super(message);
    }

    public SolrException(Throwable cause) {
        super(cause);
    }

    public SolrException(String message, Throwable cause) {
        super(message, cause);
    }
}
