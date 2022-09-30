package basicAuth.exception;

/**
 * 认证异常
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class AuthenticationException extends Exception {
    private static final long serialVersionUID = 4046517047810854243L;

    /**
     * AuthenticationException
     *
     * @param message 异常信息
     */
    public AuthenticationException(String message) {
        super(message);
    }
}
