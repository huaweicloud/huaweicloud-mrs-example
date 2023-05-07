/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.springboot.restclient.client;

import lombok.Data;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * elasticsearch配置
 *
 * @since 2022-11-14
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "elasticsearch")
public class ElasticsearchConfig {
    private String configPath;

    @Bean
    public RestHighLevelClient elasticsearchClient() {
        HwRestClient hwRestClient;
        if (!StringUtils.isEmpty(configPath)) {
            hwRestClient = new HwRestClient(configPath);
        } else {
            hwRestClient = new HwRestClient();
        }

        return new RestHighLevelClient(hwRestClient.getRestClientBuilder());
    }
}
