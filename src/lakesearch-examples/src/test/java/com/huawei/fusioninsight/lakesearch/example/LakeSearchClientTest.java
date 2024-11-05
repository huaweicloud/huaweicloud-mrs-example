/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.fusioninsight.lakesearch.example;

import com.huawei.fusioninsight.lakesearch.example.client.LakeSearchClient;
import com.huawei.fusioninsight.lakesearch.example.enums.MethodType;
import com.huawei.fusioninsight.lakesearch.example.enums.OperateType;
import com.huawei.fusioninsight.lakesearch.example.model.ResultModel;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.StatusLine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class LakeSearchClientTest {
    @Mock
    private CloseableHttpClient mockHttpClient;

    CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);

    private LakeSearchClient lakeSearchClient;

    private static String[] ipAddrs = {"ip1", "ip2", "ip3"};

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        lakeSearchClient = spy(new LakeSearchClient(ipAddrs));
        doReturn(mockHttpClient).when(lakeSearchClient).getHttpClient();
        StatusLine statusLine = new StatusLine() {
            @Override
            public ProtocolVersion getProtocolVersion() {
                return null;
            }

            @Override
            public int getStatusCode() {
                return HttpStatus.SC_OK;
            }

            @Override
            public String getReasonPhrase() {
                return null;
            }
        };
        when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(statusLine);
    }

    @Test
    void testCallWithGetMethod() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        String protocol = "http://";
        String ipPort = "24462";
        String endpoint = "v1/123/applications/456/uni-search/knowledge-repo";
        MethodType type = MethodType.GET;
        JsonElement requestContent = new JsonParser().parse("{}");
        OperateType operateType = OperateType.QUERY_KNOWLEDGE_REPO_LIST;
        String uploadFileName = null;

        ResultModel result = lakeSearchClient.call(protocol,"localhost", ipPort, endpoint, type, requestContent,
            operateType, uploadFileName);

        verify(mockHttpClient).execute(any(HttpUriRequest.class));
        assertNotNull(result);
    }

    @Test
    void testCallWithPostMethod() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        String protocol = "http://";
        String ipPort = "24462";
        String endpoint = "v1/123/applications/456/uni-search/knowledge-repo";
        MethodType type = MethodType.POST;
        JsonElement requestContent = new JsonParser().parse("{}");
        OperateType operateType = OperateType.QUERY_KNOWLEDGE_REPO_LIST;
        String uploadFileName = null;

        ResultModel result = lakeSearchClient.call(protocol,"localhost", ipPort, endpoint, type, requestContent,
            operateType, uploadFileName);

        verify(mockHttpClient).execute(any(HttpUriRequest.class));
        assertNotNull(result);
    }

    @Test
    void testCallWithDeleteMethod() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        String protocol = "http://";
        String ipPort = "24462";
        String endpoint = "v1/123/applications/456/uni-search/knowledge-repo";
        MethodType type = MethodType.DELETE;
        JsonElement requestContent = new JsonParser().parse("{}");
        OperateType operateType = OperateType.QUERY_KNOWLEDGE_REPO_LIST;
        String uploadFileName = null;

        ResultModel result = lakeSearchClient.call(protocol,"localhost", ipPort, endpoint, type, requestContent,
            operateType, uploadFileName);

        verify(mockHttpClient).execute(any(HttpUriRequest.class));
        assertNotNull(result);
    }

    @Test
    void testCallWithPutMethod() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        String protocol = "http://";
        String ipPort = "24462";
        String endpoint = "v1/123/applications/456/uni-search/knowledge-repo";
        MethodType type = MethodType.PUT;
        JsonElement requestContent = new JsonParser().parse("{}");
        OperateType operateType = OperateType.QUERY_KNOWLEDGE_REPO_LIST;
        String uploadFileName = null;

        ResultModel result = lakeSearchClient.call(protocol,"localhost", ipPort, endpoint, type, requestContent,
            operateType, uploadFileName);

        verify(mockHttpClient).execute(any(HttpUriRequest.class));
        assertNotNull(result);
    }
}
