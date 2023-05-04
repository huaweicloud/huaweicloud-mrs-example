/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.springboot.restclient.controller;

import com.huawei.fusioninsight.elasticsearch.example.springboot.restclient.service.ElasticsearchService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * elasticsearch springboot样例controller
 *
 * @since 2022-11-14
 */
@RestController
@RequestMapping("/elasticsearch")
public class ElasticsearchController {

    @Autowired
    private ElasticsearchService elasticsearchService;

    @GetMapping("/")
    public String getElasticsearch() {
        return elasticsearchService.getElasticsearchInfo();
    }

    @GetMapping("/cluster/health")
    public String clusterHealth() {
        return elasticsearchService.getElasticsearchClusterHealth();
    }

    @GetMapping("/indexByJson")
    public String indexByJson(HttpServletRequest req) {
        String indexName = req.getParameter("index");
        return elasticsearchService.indexByJson(indexName);
    }

    @GetMapping("/indexByMap")
    public String indexByMap(HttpServletRequest req) {
        String indexName = req.getParameter("index");
        return elasticsearchService.indexByMap(indexName);
    }

    @GetMapping("/indexByXContentBuilder")
    public String indexByXContentBuilder(HttpServletRequest req) {
        String indexName = req.getParameter("index");
        return elasticsearchService.indexByXContentBuilder(indexName);
    }

    @GetMapping("/update")
    public String update(HttpServletRequest req) {
        String indexName = req.getParameter("index");
        String id = req.getParameter("id");
        return elasticsearchService.update(indexName, id);
    }

    @GetMapping("/bulk")
    public String bulk(HttpServletRequest req) {
        String indexName = req.getParameter("index");
        return elasticsearchService.bulk(indexName);
    }

    @GetMapping("/getIndex")
    public String getIndex(HttpServletRequest req) {
        String indexName = req.getParameter("index");
        String id = req.getParameter("id");
        return elasticsearchService.getIndex(indexName, id);
    }

    @GetMapping("/search")
    public String search(HttpServletRequest req) {
        String indexName = req.getParameter("index");
        return elasticsearchService.search(indexName);
    }

    @GetMapping("/scroll")
    public String scroll(HttpServletRequest req) {
        String indexName = req.getParameter("index");
        String scrollId = elasticsearchService.searchScroll(indexName);
        return elasticsearchService.clearScroll(scrollId);
    }

    @GetMapping("/deleteIndex")
    public String deleteIndex(HttpServletRequest req) {
        String indexName = req.getParameter("index");
        return elasticsearchService.deleteIndex(indexName);
    }
}
