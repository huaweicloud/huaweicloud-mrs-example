package basicAuth;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.MyHttpDelete;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * HttpManager
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class HttpManager {
    private static final Logger LOG = LoggerFactory.getLogger(HttpManager.class);

    /**
     * sendHttpGetRequest
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param operationName String
     * @return 结果
     */
    public String sendHttpGetRequest(HttpClient httpClient, String operationUrl, String operationName) {
        LOG.info("Enter sendHttpGetRequest for userOperation {}.", operationName);
        HttpResponse httpResponse = null;

        if ((operationUrl == null) || (operationUrl.isEmpty())) {
            LOG.error("The operationUrl is empty.");
            return null;
        }
        if ((operationName == null) || (operationName.isEmpty())) {
            LOG.error("The operationName is empty.");
            operationName = "UserOperation";
        }

        LOG.info("The operationUrl is:{}", operationUrl);
        try {
            HttpGet httpGet = new HttpGet(operationUrl);
            httpGet.addHeader("Content-Type", "application/json;charset=UTF-8");

            httpResponse = httpClient.execute(httpGet);

            // 处理httpGet响应
            String responseLineContent = handleHttpResponse(httpResponse, operationName);
            LOG.info("SendHttpGetRequest completely.");
            return responseLineContent;
        } catch (HttpResponseException e) {
            LOG.error("HttpResponseException." + e);
        } catch (ClientProtocolException e) {
            LOG.error("ClientProtocolException." + e);
        } catch (IOException e) {
            LOG.error("IOException." + e);
        }

        return null;
    }

    /**
     * sendDownloadRequest
     * 用于下载类型的接口调用
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param operationName String
     * @return 结果
     */
    public void sendDownloadRequest(HttpClient httpClient, String operationUrl, String operationName, String fileName) {
        LOG.info("Enter sendDownloadRequest for userOperation {}.", operationName);
        HttpResponse httpResponse = null;

        if ((operationUrl == null) || (operationUrl.isEmpty())) {
            LOG.error("The operationUrl is empty.");
            return ;
        }
        if ((operationName == null) || (operationName.isEmpty())) {
            LOG.error("The operationName is empty.");
            operationName = "UserOperation";
        }

        if ((fileName == null) || (fileName.isEmpty())) {
            LOG.error("The fileName is empty.");
            return ;
        }

        LOG.info("The operationUrl is:{}", operationUrl);
        try {
            HttpGet httpGet = new HttpGet(operationUrl);
            httpGet.addHeader("Content-Type", "application/json;charset=UTF-8");

            httpResponse = httpClient.execute(httpGet);

            // 处理下载接口响应
            handleDownloadResponse(httpResponse, operationName, fileName);
            LOG.info("SendDownloadRequest completely.");
        } catch (HttpResponseException e) {
            LOG.error("HttpResponseException." + e);
        } catch (ClientProtocolException e) {
            LOG.error("ClientProtocolException." + e);
        } catch (IOException e) {
            LOG.error("IOException." + e);
        }
    }

    /**
     * sendHttpPostRequest
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param jsonFilePath  String
     * @param operationName String
     * @throws FileNotFoundException 异常
     */
    public void sendHttpPostRequest(HttpClient httpClient, String operationUrl, String jsonFilePath,
            String operationName) throws FileNotFoundException {
        LOG.info("Enter sendHttpPostRequest for userOperation {}.", operationName);

        if ((operationUrl == null) || (operationUrl.isEmpty())) {
            LOG.error("The operationUrl is empty.");
            return;
        }
        if ((operationName == null) || (operationName.isEmpty())) {
            LOG.error("The operationName is empty.");
            operationName = "userOperation";
        }
        if ((jsonFilePath == null) || (jsonFilePath.isEmpty())) {
            LOG.error("The jsonFilePath is empty.");
            return;
        }

        String filePath = jsonFilePath;
        File jsonFile = null;
        BufferedReader br = null;
        try {
            jsonFile = new File(filePath);
            List<String> jsonList = new ArrayList<String>();
            br = new BufferedReader(new FileReader(jsonFile));
            String temp = br.readLine();

            while (temp != null) {
                jsonList.add(temp);
                temp = br.readLine();
            }
            br.close();

            for (int line = 0; line < jsonList.size(); ++line) {
                String tempString = (String) jsonList.get(line);
                String json = tempString;

                if (json == null) {
                    LOG.info("sendHttpPostRequest completely.");
                    break;
                }
                // 日志不能打印敏感信息
                if (json.length() != 0) {
                    String[] strs = tempString.split(",");
                    String jsonContent = "";
                    for (int i = 0; i < strs.length; ++i) {
                        if (strs[i].contains("password")) {
                            strs[i] = "\"password\":\"XXX\"";
                        }
                        if (strs[i].contains("confirmPassword")) {
                            strs[i] = "\"confirmPassword\":\"XXX\"";
                        }

                        if (i == strs.length - 1) {
                            jsonContent = jsonContent + strs[i];
                        } else {
                            jsonContent = jsonContent + strs[i] + ",";
                        }
                    }
                    LOG.info("The json content = {}.", jsonContent);

                    HttpResponse httpResponse = null;

                    HttpPost httpPost = new HttpPost(operationUrl);
                    httpPost.addHeader("Content-Type", "application/json;charset=UTF-8");
                    httpPost.setEntity(new StringEntity(json, "UTF-8"));

                    httpResponse = httpClient.execute(httpPost);
                    handleHttpResponse(httpResponse, operationName);
                }
            }
        } catch (UnsupportedEncodingException e1) {
            LOG.error("Unsupported Encoding Exception:{}", e1.getMessage());
        } catch (ClientProtocolException e1) {
            LOG.error("Client Protocol Exception:{}", e1.getMessage());
        } catch (FileNotFoundException e) {
            LOG.error("The file does not exist");
            throw new FileNotFoundException();
        } catch (IOException e) {
            LOG.error("");
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOG.error("BufferedReader close error.");
                }
            }
        }
        LOG.info("sendHttpPostRequest completely.");
    }

    /**
     * sendHttpPostRequestWithString
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param jsonString    String
     * @param operationName String
     * @return 结果
     * @throws FileNotFoundException 异常
     */
    public String sendHttpPostRequestWithString(HttpClient httpClient, String operationUrl, String jsonString,
            String operationName) throws FileNotFoundException {
        LOG.info("Enter sendHttpPostRequest for userOperation {}.", operationName);

        if ((operationUrl == null) || (operationUrl.isEmpty())) {
            LOG.error("The operationUrl is empty.");
            return null;
        }
        if ((operationName == null) || (operationName.isEmpty())) {
            LOG.error("The operationName is empty.");
            operationName = "userOperation";
        }

        try {
            HttpResponse httpResponse = null;

            HttpPost httpPost = new HttpPost(operationUrl);
            httpPost.addHeader("Content-Type", "application/json;charset=UTF-8");
            if (StringUtils.isNotEmpty(jsonString)) {
                httpPost.setEntity(new StringEntity(jsonString, "UTF-8"));
            }

            httpResponse = httpClient.execute(httpPost);
            // 处理接口响应
            String responseLineContent = handleHttpResponse(httpResponse, operationName);
            LOG.info("Send HttpPostRequest completely.");
            return responseLineContent;
        } catch (UnsupportedEncodingException e1) {
            LOG.error("Unsupported Encoding Exception:{}.", e1.getMessage());
        } catch (ClientProtocolException e1) {
            LOG.error("Client Protocol Exception:{}.", e1.getMessage());
        } catch (IOException e) {
            LOG.error("sendHttp PostRequest error");
        }

        return null;
    }

    /**
     * sendHttpPutRequest
     *
     * @param httpclient    HttpClient
     * @param operationUrl  String
     * @param jsonFilePath  String
     * @param operationName String
     */
    public void sendHttpPutRequest(HttpClient httpclient, String operationUrl, String jsonFilePath,
            String operationName) {
        LOG.info("Enter sendHttpPutRequest for userOperation {}.", operationName);
        if ((operationUrl == null) || (operationUrl.isEmpty())) {
            LOG.error("The operationUrl is empty.");

            return;
        }
        if ((operationName == null) || (operationName.isEmpty())) {
            LOG.error("The operationName is empty.");
            operationName = "userOperation";
        }
        if ((jsonFilePath == null) || (jsonFilePath.isEmpty())) {
            LOG.error("The jsonFilePath is empty.");
        }

        String filePath = jsonFilePath;
        File jsonFile = null;
        BufferedReader br = null;
        try {
            jsonFile = new File(filePath);
            List<String> list = new ArrayList<String>();
            br = new BufferedReader(new FileReader(jsonFile));
            String temp = br.readLine();

            while (temp != null) {
                list.add(temp);
                temp = br.readLine();
            }
            br.close();

            for (int line = 0; line < list.size(); ++line) {
                String tempString = (String) list.get(line);
                String json = tempString;

                if (json == null) {
                    LOG.info("sendHttpPutRequest completely.");
                    break;
                }
                // 日志不能打印敏感信息
                if (json.length() != 0) {
                    String[] strs = tempString.split(",");
                    String jsonContent = "";
                    for (int i = 0; i < strs.length; ++i) {
                        if (strs[i].contains("password")) {
                            strs[i] = "\"password\":\"XXX\"";
                        }
                        if (strs[i].contains("confirmPassword")) {
                            strs[i] = "\"confirmPassword\":\"XXX\"";
                        }

                        if (i == strs.length - 1) {
                            jsonContent = jsonContent + strs[i];
                        } else {
                            jsonContent = jsonContent + strs[i] + ",";
                        }
                    }
                    LOG.info("The json content = {}.", jsonContent);

                    HttpResponse httpResponse = null;

                    HttpPut httpPut = new HttpPut(operationUrl);
                    httpPut.addHeader("Content-Type", "application/json;charset=UTF-8");
                    httpPut.setEntity(new StringEntity(json, "UTF-8"));

                    httpResponse = httpclient.execute(httpPut);
                    handleHttpResponse(httpResponse, operationName);

                    LOG.info("sendHttpPutRequest completely.");
                }
            }
        } catch (UnsupportedEncodingException e1) {
            LOG.error("UnsupportedEncodingException");
        } catch (ClientProtocolException e1) {
            LOG.error("ClientProtocolException");
        } catch (FileNotFoundException e) {
            LOG.info("The file does not exist");
        } catch (IOException e) {
            LOG.error("IOException");
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOG.error("IOException.BufferedReader close error.");
                }
            }
        }
    }

    /**
     * sendHttpPutRequestWithString
     *
     * @param httpclient    HttpClient
     * @param operationUrl  String
     * @param jsonString    String
     * @param operationName String
     */
    public void sendHttpPutRequestWithString(HttpClient httpclient, String operationUrl, String jsonString,
            String operationName) {
        LOG.info("Enter sendHttpPutRequest for userOperation {}.", operationName);
        if ((operationUrl == null) || (operationUrl.isEmpty())) {
            LOG.error("The operationUrl is empty.");

            return;
        }
        if ((operationName == null) || (operationName.isEmpty())) {
            LOG.error("The operationName is empty.");
            operationName = "userOperation";
        }

        try {

            HttpResponse httpResponse = null;

            HttpPut httpPut = new HttpPut(operationUrl);
            httpPut.addHeader("Content-Type", "application/json;charset=UTF-8");
            if (StringUtils.isNotEmpty(jsonString)) {
                httpPut.setEntity(new StringEntity(jsonString, "UTF-8"));
            }

            httpResponse = httpclient.execute(httpPut);
            handleHttpResponse(httpResponse, operationName);

            LOG.info("sendHttpPutRequest completely.");
        } catch (UnsupportedEncodingException e1) {
            LOG.error("UnsupportedEncodingException");
        } catch (ClientProtocolException e1) {
            LOG.error("ClientProtocolException");
        } catch (IOException e) {
            LOG.error("IOException");
        }
    }

    /**
     * sendHttpDeleteRequest
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param jsonString    String
     * @param operationName String
     */
    public void sendHttpDeleteRequest(HttpClient httpClient, String operationUrl, String jsonString,
            String operationName) {
        if ((operationUrl == null) || (operationUrl.isEmpty())) {
            LOG.error("The operationUrl is empty.");
            return;
        }
        if ((operationName == null) || (operationName.isEmpty())) {
            LOG.error("The operationName is empty.");
            operationName = "UserOperation";
        }

        LOG.info("The operationUrl is:{}", operationUrl);
        LOG.info("Enter sendHttpDeleteMessage for operation {}.", operationName);
        try {
            HttpResponse httpResponse = null;

            if (StringUtils.isEmpty(jsonString)) {
                HttpDelete httpDelete = new HttpDelete(operationUrl);
                httpResponse = httpClient.execute(httpDelete);
            } else {
                MyHttpDelete myHttpDelete = new MyHttpDelete(operationUrl);
                myHttpDelete.addHeader("Content-Type", "application/json;charset=UTF-8");
                myHttpDelete.setEntity(new StringEntity(jsonString, "UTF-8"));
                httpResponse = httpClient.execute(myHttpDelete);
            }

            handleHttpResponse(httpResponse, operationName);

            LOG.info("sendHttpDeleteMessage for {} completely.", operationName);
        } catch (ClientProtocolException e1) {
            LOG.error("ClientProtocolException");
        } catch (IOException e) {
            LOG.error("IOException");
        } catch (Exception e) {
            LOG.error("Exception");
        }
    }

    private String handleHttpResponse(HttpResponse httpResponse, String operationName) {
        String lineContent = "";
        if (httpResponse == null) {
            LOG.error("The httpResponse is empty.");
        }
        BufferedReader bufferedReader = null;
        InputStream inputStream = null;
        try {
            LOG.info("The {} status is {}.", operationName, httpResponse.getStatusLine());
            if (httpResponse.getEntity() != null && httpResponse.getEntity().getContent() != null) {
                inputStream = httpResponse.getEntity().getContent();
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                lineContent = bufferedReader.readLine();
                LOG.info("The response lineContent is {}.", lineContent);
            }
        } catch (IOException e) {
            LOG.warn("ReadLine failed.");
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    LOG.info("Close bufferedReader failed.");
                }
            }
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOG.info("Close inputStream failed.");
                }
            }
        }
        return lineContent;
    }

    private void handleDownloadResponse(HttpResponse httpResponse, String operationName, String fileName) {
        if (httpResponse == null) {
            LOG.error("The httpResponse is empty.");
            return;
        }
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            LOG.info("The {} status is {}.", operationName, httpResponse.getStatusLine());
            if (httpResponse.getEntity() != null && httpResponse.getEntity().getContent() != null) {
                inputStream = httpResponse.getEntity().getContent();
                // 处理下载类接口
                if (httpResponse.getEntity().getContentType() != null && httpResponse.getEntity()
                        .getContentType()
                        .toString()
                        .contains("application/x-download")) {
                    File file = new File(fileName);
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    outputStream = new FileOutputStream(file);
                    int length;
                    byte[] buffer = new byte[1024];
                    while ((length = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, length);
                    }
                }
                LOG.info("File {} download successful.", fileName);
            }
        } catch (IOException e) {
            LOG.warn("ReadLine failed.");
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOG.info("Close inputStream failed.");
                }
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    LOG.info("Close outputStream failed.");
                }
            }
        }
    }
}
