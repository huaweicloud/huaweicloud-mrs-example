/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.fusioninsight.lakesearch.example;

import com.huawei.fusioninsight.lakesearch.example.client.LakeSearchClient;
import com.huawei.fusioninsight.lakesearch.example.enums.MethodType;
import com.huawei.fusioninsight.lakesearch.example.enums.OperateType;
import com.huawei.fusioninsight.lakesearch.example.model.ResultModel;
import com.huawei.us.common.file.UsFileUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Map;
import java.util.Objects;
import java.util.HashMap;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * LakeSearch样例代码
 *
 * @since 2024-06-13
 */
public class TestExample {
    private static final Logger LOG = LoggerFactory.getLogger(TestExample.class);

    private static String[] ipAddrs;

    private static String ipPort;

    private static boolean isSecEnabled;

    private static String protocol = "http://";

    private static String principal;

    private static String userKeytabFile;

    private static String krb5File;

    private static String createRepoName;

    private static String embeddingModelName;

    private static String rerankModelName;

    private static String panguNlpModelName;

    private static final String projectId = "1ed40ceefc8d40f8b884edb6a84e7768";

    private static final String applicationId = "fb9731ab-7085-474f-b6c7-64473586f0f3";

    private static String createRepoId = null;

    private static Subject subject;

    private static final Pattern pattern = Pattern.compile(".*");

    private static String confPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;;

    public static void main(String[] args) throws Exception {
        initProperties();
        if (isSecEnabled) {
            doKerberosLogin();
            protocol = "https://";
        }

        LakeSearchClient client = new LakeSearchClient(ipAddrs);
        createKnowledgeRepo(createRepoName, client);
        queryKnowledgeRepoList(client);
        modifyKnowledgeRepo(createRepoId, client);
        queryKnowledgeRepoList(client);
        uploadFiles(client);
        bulkImportFaq(client);
        uploadStructuredData(client);
        queryFilesStatus(client);
        searchText(client);
    }

    private static void doKerberosLogin() throws IOException, LoginException {
        prepareKerberosProperties();
        login(principal, userKeytabFile, krb5File);
    }

    private static void prepareKerberosProperties() throws IOException {
        confPath = confPath.replace("\\", "\\\\");
        userKeytabFile = confPath + "user.keytab";
        krb5File = confPath + "krb5.conf";
        principal = KerberosUtil.getPrincipalNames(userKeytabFile, pattern)[0];
    }

    private static void createKnowledgeRepo(String repoName, LakeSearchClient client) {
        String endpoint = "/v1/" + projectId + "/applications/" + applicationId + "/uni-search/knowledge-repo";
        //需要指定知识库名称name、各个模型的名称
        String requestBody = "{\"name\":\"" + repoName + "\",\"language_id\":\"zh\",\"detail\":\"\",\"file_extract\":" +
            "{\"parse_conf\":{\"ocr_enabled\":false,\"image_enabled\":false,\"header_footer_enabled\":false," +
            "\"catalog_enabled\":false},\"split_conf\":{\"split_mode\":\"AUTO\"}}," +
            "\"extend_config\": { \"extend_context\": false, \"effective_input_length\": 3 }," +
            "\"embedding_model\":\"" + embeddingModelName +"\",\"rerank_model\":\"" + rerankModelName + "\"," +
            "\"pangu_nlp_model\":\"" + panguNlpModelName + "\",\"cache_enabled\":false," +
            "\"answer_reference_enabled\":false,\"answer_image_reference_enabled\":false}";
        JsonElement requestBodyJson = JsonParser.parseString(requestBody);
        Optional<ResultModel> result = client.sendAction(subject, protocol, ipPort, endpoint, MethodType.POST,
            requestBodyJson, OperateType.CREATE_KNOWLEDGE_REPO, null);
        createRepoId = result.get().getContent().get("repo_id").toString().replace("\"", "");
    }

    private static void modifyKnowledgeRepo(String repoId, LakeSearchClient client) {
        String endpoint = "/v1/" + projectId + "/applications/" + applicationId + "/uni-search/knowledge-repo/" + repoId;
        //修改reference_count为2
        String requestBody = "{\"file_extract\":{\"parse_conf\":{\"ocr_enabled\":false,\"image_enabled\":false," +
            "\"header_footer_enabled\":false,\"catalog_enabled\":false},\"split_conf\":{\"split_mode\":\"AUTO\"}}," +
            "\"top_k\":50,\"reference_count\":2,\"faq_threshold\":0.95,\"rerank_enabled\":true," +
            "\"query_rewrite_enabled\":true,\"search_plan_category_ids\":[],\"search_threshold\":0," +
            "\"chat_ref_threshold\":0,\"cache_enabled\":false,\"answer_reference_enabled\":false," +
            "\"answer_image_reference_enabled\":false," + "\"extend_config\": { \"extend_context\": false, " +
            "\"effective_input_length\": 3 },"  + "\"embedding_model\":\"" + embeddingModelName + "\"," +
            "\"rerank_model\":\"" + rerankModelName + "\",\"pangu_nlp_model\":\"" + panguNlpModelName + "\"}";
        JsonElement requestBodyJson = JsonParser.parseString(requestBody);
        Optional<ResultModel> result = client.sendAction(subject, protocol, ipPort, endpoint, MethodType.PUT, requestBodyJson,
            OperateType.MODIFY_KNOWLEDGE_REPO, null);
    }

    private static void queryKnowledgeRepoList(LakeSearchClient client) {
        String endpoint = "/v1/" + projectId + "/applications/" + applicationId + "/uni-search/knowledge-repo";
        Optional<ResultModel> result = client.sendAction(subject, protocol, ipPort, endpoint, MethodType.GET,
            null, OperateType.QUERY_KNOWLEDGE_REPO_LIST, null);
    }

    private static void uploadFiles(LakeSearchClient client) {
        String endpoint = "/v1/" + projectId + "/applications/" + applicationId + "/uni-search/" + createRepoId + "/files";
        Optional<ResultModel> result = client.sendAction(subject, protocol, ipPort, endpoint, MethodType.POST,
            null, OperateType.UPLOAD_FILES, "word-example.docx");
    }

    private static void bulkImportFaq(LakeSearchClient client) {
        String endpoint = "/v1/" + projectId + "/applications/" + applicationId + "/uni-search/" + createRepoId + "/faq/batch/upload";
        Optional<ResultModel> result = client.sendAction(subject, protocol, ipPort, endpoint, MethodType.POST,
            null, OperateType.BULK_IMPORT_FAQ, "faq_sample.xlsx");
    }

    private static void uploadStructuredData(LakeSearchClient client) {
        String endpoint = "/v1/" + projectId + "/applications/" + applicationId + "/uni-search/" + createRepoId + "/structured-data";
        Optional<ResultModel> result = client.sendAction(subject, protocol, ipPort, endpoint, MethodType.POST,
            null, OperateType.UPLOAD_STRUCTURED_DATA, "template_structuredData_file.jsonl");
    }

    private static void searchText(LakeSearchClient client) {
        String endpoint = "/v1/" + projectId + "/applications/" + applicationId + "/uni-search/experience/searchtext";

        String requestBody = "{\"repo_id\":\"" + createRepoId + "\",\"content\":\"garbage collector\"," +
            "\"page_num\":1,\"page_size\":10,\"scope\":\"doc\"}";
        JsonElement requestBodyJson = JsonParser.parseString(requestBody);

        Optional<ResultModel> result = client.sendAction(subject, protocol, ipPort, endpoint, MethodType.POST,
            requestBodyJson, OperateType.SEARCHTEXT, null);
    }

    private static void queryFilesStatus(LakeSearchClient client) {
        String endpoint = "/v1/" + projectId + "/applications/" + applicationId + "/uni-search/" + createRepoId + "/files/search";
        boolean isUploadSuccess = false;
        int queryCount = 0;

        while (!isUploadSuccess) {
            queryCount++;
            if (queryCount > 50) {
                LOG.error("Query upload file status exceeds limit!");
                break;
            }
            Optional<ResultModel> result = client.sendAction(subject, protocol, ipPort, endpoint, MethodType.GET,
                null, OperateType.QUERY_FILES_STATUS, null);
            String jsonStr = null;
            if (result.isPresent()) {
                jsonStr = result.get().getContent().toString();
            }
            JSONObject jsonObj = JSONObject.parseObject(jsonStr);
            JSONArray filesArr = Objects.requireNonNull(jsonObj).getJSONArray("files");
            JSONObject fileObj = filesArr.getJSONObject(0);
            LOG.info("Status for the upload file is: {} ", fileObj.getString("status"));
            if ("SUCCESS".equals(fileObj.getString("status"))) {
                isUploadSuccess = true;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void login(String principal, String userKeytabFile, String krb5File) throws LoginException {
        Map<String, String> options = new HashMap<>();
        options.put("useTicketCache", "false");
        options.put("useKeyTab", "true");
        options.put("keyTab", userKeytabFile);

        /**
         * Krb5 in GSS API needs to be refreshed so it does not throw the error
         * Specified version of key is not available
         */

        options.put("refreshKrb5Config", "true");
        options.put("principal", principal);
        options.put("storeKey", "true");
        options.put("doNotPrompt", "true");
        options.put("isInitiator", "true");
        options.put("debug", "true");
        System.setProperty("java.security.krb5.conf", krb5File);
        Configuration config = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {
                    new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)
                };
            }
        };
        subject = new Subject(false, Collections.singleton(new KerberosPrincipal(principal)), Collections.EMPTY_SET,
            Collections.EMPTY_SET);
        LoginContext loginContext = new LoginContext("Krb5Login", subject, null, config);
        loginContext.login();
    }

    private static void initProperties() {
        Properties properties = new Properties();
        String proPath = confPath + "lakesearch-example.properties";

        try {
            properties.load(Files.newInputStream(UsFileUtils.getFile(proPath).toPath()));
        } catch (IOException e) {
            LOG.error("Failed to load properties file.", e);
        }

        ipAddrs = properties.getProperty("ipAddrs").split(",");
        ipPort = properties.getProperty("server.port");
        isSecEnabled = Boolean.parseBoolean(properties.getProperty("lakesearchSecEnable"));
        createRepoName = properties.getProperty("createRepoName");
        embeddingModelName = properties.getProperty("embedding_model");
        rerankModelName = properties.getProperty("rerank_model");
        panguNlpModelName = properties.getProperty("pangu_nlp_model");
    }
}
