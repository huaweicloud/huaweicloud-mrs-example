/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.model;

import java.util.Date;

/**
 * 样例所需的实体类
 *
 * @since 2020-09-15
 */
public class Article {
    private int id;

    private String title;

    private String content;

    private String url;

    private Date pubdate;

    private String source;

    private String author;

    /**
     * 构造方法
     */
    public Article() {
        this(1, null, "", null, new Date(), null, "Linda");
    }

    public Article(int id, String title, String content, String url, Date pubdate, String source, String author) {
        super();
        this.id = id;
        this.title = title;
        this.content = content;
        this.url = url;
        this.pubdate = pubdate;
        this.source = source;
        this.author = author;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Date getPubdate() {
        return pubdate;
    }

    public void setPubdate(Date pubdate) {
        this.pubdate = pubdate;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }
}
