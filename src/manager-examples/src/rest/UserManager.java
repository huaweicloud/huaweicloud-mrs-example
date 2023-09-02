package rest;

import basicAuth.BasicAuthAccess;
import basicAuth.HttpManager;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.PropertiesUtil;

/**
 * 增删改查操作Demo,包括用户的增删改查
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

        // 获取用户名
        String userName = PropertiesUtil.getInstance().getUserName();
        if (userName == null || userName.isEmpty()) {
            LOG.error("The userName is empty.");
            return;
        }

        // 获取用户密码
        String password = PropertiesUtil.getInstance().password();
        if (password == null || password.isEmpty()) {
            LOG.error("The password is empty.");
            return;
        }

        String webUrl = PropertiesUtil.getInstance().getWebUrl();
        if (password == null || password.isEmpty()) {
            LOG.error("The password is empty.");
            return;
        }

        // userTLSVersion是必备的参数，是处理jdk1.6服务端连接jdk1.8服务端时的重要参数，如果用户使用的是jdk1.8该参数赋值为空字符串即可
        String userTLSVersion = "";
        try {
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

        } catch (Exception e) {
            LOG.error("userManager run error:{}", e.getMessage());
        }
    }
}
