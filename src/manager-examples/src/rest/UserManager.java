package rest;

import basicAuth.BasicAuthAccess;
import basicAuth.HttpManager;
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
 * HttpClient自带的HttpDelete方法是不支持上传body的，所以重写delete方法
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class UserManager {
    private static final Logger LOG = LoggerFactory.getLogger(UserManager.class);

    private static final String ADD_USER_URL = "api/v2/permission/users";

    private static final String QUERY_USER_LIST_URL = "api/v2/permission/users?limit=10&offset=0&filter=&order=ASC"
            + "&order_by=userName";

    private static final String MODIFY_USER_URL = "api/v2/permission/users/";

    private static final String DELETE_USER_URL = "api/v2/permission/users";

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

            HttpManager httpManager = new HttpManager();
            String operationName = "";
            String operationUrl = "";
            String jsonFilePath = "";

            // 访问Manager接口完成添加用户
            operationName = "AddUser";
            operationUrl = webUrl + ADD_USER_URL;
            jsonFilePath = "./conf/addUser.json";
            httpManager.sendHttpPostRequest(httpClient, operationUrl, jsonFilePath, operationName);

            // 访问Manager接口完成查找用户列表
            operationName = "QueryUserList";
            operationUrl = webUrl + QUERY_USER_LIST_URL;
            String responseLineContent = httpManager.sendHttpGetRequest(httpClient, operationUrl, operationName);
            LOG.info("The {} response is {}.", operationName, responseLineContent);

            // 访问Manager接口完成修改用户
            operationName = "ModifyUser";
            String modifyUserName = "user888";
            operationUrl = webUrl + MODIFY_USER_URL + modifyUserName;
            jsonFilePath = "./conf/modifyUser.json";
            httpManager.sendHttpPutRequest(httpClient, operationUrl, jsonFilePath, operationName);

            // 访问Manager接口完成删除用户
            operationName = "DeleteUser";
            String deleteJsonStr = "{\"userNames\":[\"user888\"]}";
            operationUrl = webUrl + DELETE_USER_URL;
            httpManager.sendHttpDeleteRequest(httpClient, operationUrl, deleteJsonStr, operationName);

            LOG.info("Exit main.");

        } catch (FileNotFoundException e) {
            LOG.error("File not found exception.");
        } catch (IOException e) {
            LOG.error("userManager run error:{}", e.getMessage());
        } catch (Throwable e) {
            LOG.error("userManager run error:{}", e.getMessage());
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
