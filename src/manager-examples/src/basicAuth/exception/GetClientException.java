package basicAuth.exception;

/**
 * 获取客户端异常
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class GetClientException extends Exception {
    private static final long serialVersionUID = 4046517047810854241L;

    public GetClientException(String message) {
        super(message);
    }
}
