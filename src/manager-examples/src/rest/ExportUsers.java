package rest;

import basicAuth.BasicAuthAccess;
import basicAuth.HttpManager;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.PropertiesUtil;

/**
 * 导出接口Demo,包括导出用户和下载用户两个接口。一个完成的导出操作，需同时调用导出接口和下载接口两个接口。
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
            String fileName = jsonObj.getString("fileName");
            String downloadOperationUrl = webUrl + DOWNLOAD_URL + fileName;
            // 文件下载到本地的路径（此处举例为C盘根目录，开发时根据实际情况定义）
            String downloadFilePath = "C:\\";
            httpManager.sendDownloadRequest(httpClient, downloadOperationUrl, operationName,
                    downloadFilePath + fileName);

            LOG.info("Exit main.");

        } catch (Exception e) {
            LOG.error("Export Users running exception:{}", e.getMessage());
        }
    }
}
