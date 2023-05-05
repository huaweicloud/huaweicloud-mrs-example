/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.fusioninsight.solr.performance;

/**
 * 自定义的SolrException异常
 *
 * @since 2020-03-04
 */
public class SolrException extends Exception {

    static final long serialVersionUID = 0;

    public SolrException(String message) {
        super(message);
    }

    public SolrException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
