package basicAuth;

import basicAuth.exception.AuthenticationException;
import basicAuth.exception.FirstTimeAccessException;
import basicAuth.exception.GetClientException;
import basicAuth.exception.InvalidInputParamException;
import basicAuth.exception.WrongUsernameOrPasswordException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.WebClientDevWrapper;
import validutil.ParamsValidUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Base64;

/**
 * BasicAuthAccess
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class BasicAuthAccess {
    private static final Logger LOG = LoggerFactory.getLogger(BasicAuthAccess.class);

    /**
     * loginAndAccess
     *
     * @param webUrl         url
     * @param userName       用户名
     * @param password       密码
     * @param userTLSVersion String
     * @return 响应结果
     * @throws InvalidInputParamException       异常
     * @throws GetClientException               异常
     * @throws WrongUsernameOrPasswordException 异常
     * @throws FirstTimeAccessException         异常
     * @throws AuthenticationException          异常
     */
    public HttpClient loginAndAccess(String webUrl, String userName, String password, String userTLSVersion)
            throws InvalidInputParamException, GetClientException, WrongUsernameOrPasswordException,
            FirstTimeAccessException, AuthenticationException {
        LOG.info("Enter loginAndAccess.");
        if (ParamsValidUtil.isEmpty(new String[] {webUrl, userName, password})) {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
        }

        if ((userTLSVersion == null) || (userTLSVersion.isEmpty())) {
            userTLSVersion = "TLS";
        }

        LOG.info("1.Get http client for sending https request, username is {}, webUrl is {}.", new Object[] {
                userName, webUrl
        });
        HttpClient httpClient = getHttpClient(userTLSVersion);
        LOG.info("The new http client is: {}.", httpClient);
        if (ParamsValidUtil.isNull(new Object[] {httpClient})) {
            LOG.error("Get http client error.");
            throw new GetClientException("Get http client error.");
        }

        LOG.info("2.Construct basic authentication,username is {}.", userName);
        // 使用password认证
        String credentials = password;
        // 使用keytab认证
        // String credentials = getKeytabContent("E:\\user.keytab");
        String authentication = constructAuthentication(userName, credentials);
        if (ParamsValidUtil.isNull(new Object[] {authentication})) {
            LOG.error("Authroize failed.");
        }

        LOG.info("3. Send first access request, usename is {}.", userName);
        HttpResponse firstAccessResp = firstAccessResp(webUrl, userName, authentication, httpClient);

        if (ParamsValidUtil.isNull(new Object[] {firstAccessResp})) {
            LOG.error("First access response error.");
            throw new FirstTimeAccessException("First access response error.");
        }

        return httpClient;
    }

    private HttpClient getHttpClient(String userTLSVersion) {
        LOG.info("Enter getHttpClient.");

        ThreadSafeClientConnManager ccm = new ThreadSafeClientConnManager();
        ccm.setMaxTotal(100);

        HttpClient httpclient = WebClientDevWrapper.wrapClient(new DefaultHttpClient(ccm), userTLSVersion);
        LOG.info("Exit getHttpClient.");
        return httpclient;
    }

    private String constructAuthentication(String userName, String credentials) throws FirstTimeAccessException {
        StringBuffer sb = new StringBuffer();
        sb.append("Basic");
        sb.append(" ");

        String userNamePasswordToken = userName + ":" + credentials;
        try {
            byte[] token64 = Base64.getEncoder().encode(userNamePasswordToken.getBytes("UTF-8"));
            String token = new String(token64);
            sb.append(token);
        } catch (UnsupportedEncodingException e) {
            LOG.error("First access failed because of UnsupportedEncodingException.");
            throw new FirstTimeAccessException("UnsupportedEncodingException");
        }
        return sb.toString();
    }

    public static String getKeytabContent(String keytabPath) {
        FileInputStream inputStream = null;
        String keytabContent = null;
        try {
            File file = new File(keytabPath);
            Long fileLen = file.length();
            byte[] tmp = new byte[fileLen.intValue()];
            inputStream = new FileInputStream(keytabPath);
            inputStream.read(tmp);
            keytabContent = Base64.getEncoder().encodeToString(tmp);
        } catch (IOException e) {
            return null;
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                }
            }
        }
        return keytabContent;
    }

    private HttpResponse firstAccessResp(String webUrl, String userName, String authentication, HttpClient httpClient)
            throws WrongUsernameOrPasswordException, FirstTimeAccessException, AuthenticationException {
        HttpGet httpGet = new HttpGet(webUrl + "api/v2/session/status");
        httpGet.addHeader("Authorization", authentication);
        BufferedReader bufferedReader = null;

        HttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            int stateCode = response.getStatusLine().getStatusCode();
            LOG.info("First access status is {}", stateCode);

            InputStream inputStream = response.getEntity().getContent();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String lineContent = "";
            lineContent = bufferedReader.readLine();
            LOG.info("Response content is {} ", lineContent);

            if (stateCode != HttpStatus.SC_OK) {
                throw new AuthenticationException("Authorize failed!");
            }

            LOG.info("User {} first access success", userName);

            while (lineContent != null) {
                LOG.debug("lineContent={}", lineContent);

                if (lineContent.contains("The credentials you provided cannot be determined to be authentic")) {
                    LOG.error("The username or password is wrong");
                    throw new WrongUsernameOrPasswordException("The username or password is wrong");
                }

                if (lineContent.contains("modify_password.html")) {
                    LOG.warn("First access, please reset password");
                    throw new FirstTimeAccessException("First access, please reset password");
                }
                lineContent = bufferedReader.readLine();
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("First access server failed because of UnsupportedEncodingException.");
            throw new FirstTimeAccessException("UnsupportedEncodingException");
        } catch (ClientProtocolException e) {
            LOG.error("First access server failed because of FirstTimeAccessException.");
            throw new FirstTimeAccessException("ClientProtocolException");
        } catch (IOException e) {
            LOG.error("First access server failed because of ClientProtocolException.");
            throw new FirstTimeAccessException("IOException");
        } catch (IllegalStateException e) {
            LOG.error("First access server failed because of IllegalStateException.");
            throw new FirstTimeAccessException("IllegalStateException");
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    LOG.warn("Close buffer reader failed.");
                }
            }
        }
        return response;
    }
}
