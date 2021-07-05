package rest;

import basicAuth.BasicAuthAccess;
import basicAuth.HttpManager;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

/**
 * ExportUsers
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class ExportUsers {
    private static final Logger LOG = LoggerFactory.getLogger(ExportUsers.class);

    private static final String EXPORT_URL = "api/v2/permission/users/operations/export?filter=&format=txt";

    private static final String DOWNLOAD_URL = "api/v2/permission/users/download?file_name=";

    /**
     * 程序运行入口
     *
     * @param args 参数
     */
    public static void main(String[] args) {
        LOG.info("Enter main.");
        // 文件UserInfo.properties的路径
        String userFilePath = "./conf/UserInfo.properties";

        InputStream userInfo = null;
        ResourceBundle resourceBundle = null;
        try {
            File file = new File(userFilePath);
            if (!file.exists()) {
                LOG.error("The user info file doesn't exist.");
                return;
            }

            LOG.info("Get the web info and user info from file {} ", file);

            userInfo = new BufferedInputStream(new FileInputStream(file));
            resourceBundle = new PropertyResourceBundle(userInfo);

            // 获取用户名
            String userName = resourceBundle.getString("userName");
            LOG.info("The user name is : {}.", userName);
            if (userName == null || userName.isEmpty()) {
                LOG.error("The userName is empty.");
            }

            // 获取用户密码
            String password = resourceBundle.getString("password");
            if (password == null || password.isEmpty()) {
                LOG.error("The password is empty.");
            }

            String webUrl = resourceBundle.getString("webUrl");
            LOG.info("The webUrl is : {}.", webUrl);
            if (password == null || password.isEmpty()) {
                LOG.error("The password is empty.");
            }

            // userTLSVersion是必备的参数，是处理jdk1.6服务端连接jdk1.8服务端时的重要参数，如果用户使用的是jdk1.8该参数赋值为空字符串即可
            String userTLSVersion = "";

            // 调用firstAccess接口完成登录认证
            LOG.info("Begin to get httpclient and first access.");
            BasicAuthAccess authAccess = new BasicAuthAccess();
            HttpClient httpClient = authAccess.loginAndAccess(webUrl, userName, password, userTLSVersion);

            LOG.info("Start to access REST API.");

            // 访问Manager接口完成导出用户
            String operationName = "ExportUsers";
            String exportOperationUrl = webUrl + EXPORT_URL;
            HttpManager httpManager = new HttpManager();
            // 调用导出接口
            String responseLineContent = httpManager.sendHttpPostRequestWithString(httpClient, exportOperationUrl,
                    StringUtils.EMPTY, operationName);
            // 调用下载接口
            operationName = "DownloadUsers";
            JSONObject jsonObj = JSON.parseObject(responseLineContent);
            String downloadOperationUrl = webUrl + DOWNLOAD_URL + jsonObj.getString("fileName");
            httpManager.sendHttpGetRequest(httpClient, downloadOperationUrl, operationName);

            LOG.info("Exit main.");

        } catch (FileNotFoundException e) {
            LOG.error("File not found exception.");
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        } catch (Throwable e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        } finally {
            if (userInfo != null) {
                try {
                    userInfo.close();
                } catch (IOException e) {
                    LOG.error("IOException.");
                }
            }
        }
    }
}
