package utils;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;

/**
 * HttpClient自带的HttpDelete方法是不支持上传body的，所以重写delete方法
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class MyHttpDelete extends HttpEntityEnclosingRequestBase {
    /**
     * 删除模式
     */
    public static final String METHOD_NAME = "DELETE";

    /**
     * MyHttpDelete
     *
     * @param uri URL
     */
    public MyHttpDelete(final String uri) {
        super();
        setURI(URI.create(uri));
    }

    /**
     * MyHttpDelete
     *
     * @param uri URI
     */
    public MyHttpDelete(final URI uri) {
        super();
        setURI(uri);
    }

    /**
     * MyHttpDelete
     */
    public MyHttpDelete() {
        super();
    }

    /**
     * getMethod
     *
     * @return String
     */
    public String getMethod() {
        return METHOD_NAME;
    }
}
