/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
package com.huawei.fusioninsight.es.performance;

public class ConfigInfo {
    final String httpUrl;
    long processRecordNum;
    final String index;
    final String type;
    final int threadNum;
    final int isSecurity;
    final int isIndex;
    final int connectTimeout;
    final int socketTimeout;
    final int singleRecordData;
    final String queryType;

    public static class Builder {
        private String httpUrl;
        private long processRecordNum;
        private String index;
        private String type;
        private int threadNum;
        private int isSecurity;
        private int isIndex;
        private int connectTimeout;
        private int socketTimeout;
        private int singleRecordData;
        private String queryType;

        public Builder(){

        }
        public Builder setHttpUrl(String httpUrl){
            this.httpUrl = httpUrl;
            return this;
        }
        public Builder setProcessRecordNum(long processRecordNum){
            this.processRecordNum = processRecordNum;
            return this;
        }
        public Builder setIndex(String index){
            this.index = index;
            return this;
        }
        public Builder setType(String type){
            this.type = type;
            return this;
        }
        public Builder setThreadNum(int threadNum){
            this.threadNum = threadNum;
            return this;
        }
        public Builder setIsSecurity(int isSecurity){
            this.isSecurity = isSecurity;
            return this;
        }
        public Builder setIsIndex(int isIndex){
            this.isIndex = isIndex;
            return this;
        }
        public Builder setConnectTimeout(int connectTimeout){
            this.connectTimeout = connectTimeout;
            return this;
        }
        public Builder setSocketTimeout(int socketTimeout){
            this.socketTimeout = socketTimeout;
            return this;
        }
        public Builder setSingleRecordData(int singleRecordData){
            this.singleRecordData = singleRecordData;
            return this;
        }
        public Builder setQueryType(String queryType){
            this.queryType = queryType;
            return this;
        }
        public ConfigInfo build() {
            return new ConfigInfo(this);
        }
    }

    private ConfigInfo(Builder builder) {
        this.httpUrl = builder.httpUrl;
        this.processRecordNum = builder.processRecordNum;
        this.index = builder.index;
        this.type = builder.type;
        this.threadNum = builder.threadNum;
        this.isSecurity = builder.isSecurity;
        this.isIndex = builder.isIndex;
        this.connectTimeout = builder.connectTimeout;
        this.socketTimeout = builder.socketTimeout;
        this.singleRecordData = builder.singleRecordData;
        this.queryType = builder.queryType;
    }
}


