/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.solr;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;

/**
 *
 * @since 2020-09-30
 */
public class SolrJsonSpout extends BaseRichSpout {
    private static final long serialVersionUID = -265528709717019031L;
    private SpoutOutputCollector collector;
    private static final List<Values> LIST_VALUES =
            Lists.newArrayList(
                    getJsonValues("1"),
                    getJsonValues("2"),
                    getJsonValues("3"), // Tuple contains String Object in JSON format
                    getPojoValues("1"),
                    getPojoValues("2")); // Tuple contains Java object that must be serialized to JSON by SolrJsonMapper

    /**
     * @param conf  配置
     * @param context 上下文
     * @param collector collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     *
     */
    @Override
    public void nextTuple() {
        final SecureRandom rand = new SecureRandom();
        final Values values = LIST_VALUES.get(rand.nextInt(LIST_VALUES.size()));
        collector.emit(values);
        Thread.yield();
    }

    /**
     *
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }

    /**
     * @return Fields
     */
    public Fields getOutputFields() {
        return new Fields("JSON");
    }

    /**
     *
     */
    @Override
    public void close() { // TODO
        super.close();
    }

    // ====

    /**
     * @param suf String
     * @return Values
     */
    private static Values getJsonValues(String suf) {
        String suffix = "_json_test_val_" + suf;
        return new Values((new JsonSchema(suffix)).toJson());
    }

    private static Values getPojoValues(String suf) {
        String suffix = "_json_test_val_" + suf;
        return new Values(new JsonSchema(suffix));
    }

    /**
     *
     */
    public static class JsonSchema {
        private static final Logger logger = Logger.getLogger(JsonSchema.class);
        private String id;
        private String date;
        private String dc_title;

        private static final Gson GSON = new Gson();

        public JsonSchema(String suffix) {
            this.id = "id" + suffix;
            this.date = TestUtil.getDate();
            this.dc_title = "dc_title" + suffix;
        }

        public JsonSchema(String id, String date, String dc_title) {
            this.id = id;
            this.date = date;
            this.dc_title = dc_title;
        }

        /**
         *
         */
        public JsonSchema(JsonSchema jsonSchema) {
            this.id = jsonSchema.id;
            this.date = jsonSchema.date;
            this.dc_title = jsonSchema.dc_title;
        }

        /**
         * @return String
         */
        public String toJson() {
            String json = GSON.toJson(this);
            logger.info(String.valueOf(json)); // TODO log
            return json;
        }

        /**
         * @param jsonStr json string
         * @return JsonSchema
         */
        public static JsonSchema fromJson(String jsonStr) {
            return new JsonSchema(GSON.fromJson(jsonStr, JsonSchema.class));
        }
    }
}
