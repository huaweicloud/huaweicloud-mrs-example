package com.huawei.graphbase.rest.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class RestHelper {

    public static String toJsonString(Object o) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return objectMapper.writeValueAsString(o);
    }

    public static void checkHttpRsp(CloseableHttpResponse response) throws Exception {
        if (null == response) {
            throw new Exception("Http response error: response is null.");
        }

        if (HttpStatus.SC_OK != response.getStatusLine().getStatusCode()) {
            throw new Exception("Http response error: " + response.getStatusLine() + "\n " + inputStreamToStr(
                response.getEntity().getContent()));
        }
    }

    public static String inputStreamToStr(InputStream in) {
        if (null == in) {
            return null;
        }

        StringBuffer strBuf = new StringBuffer("");
        BufferedReader bufferedReader = null;

        try {
            bufferedReader = new BufferedReader(new InputStreamReader(in));
            String lineContent = bufferedReader.readLine();
            while (lineContent != null) {
                strBuf.append(lineContent);
                lineContent = bufferedReader.readLine();
            }
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
        } finally {
            if (null != bufferedReader) {
                try {
                    bufferedReader.close();
                } catch (IOException ignore) {
                    // to do nothing.
                }
            }
        }
        return strBuf.toString();
    }
}
