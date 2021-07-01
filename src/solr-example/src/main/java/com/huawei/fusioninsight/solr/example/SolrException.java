/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.fusioninsight.solr.example;

/**
 * 自定义的SolrException异常
 *
 * @since 2020-03-04
 */
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
