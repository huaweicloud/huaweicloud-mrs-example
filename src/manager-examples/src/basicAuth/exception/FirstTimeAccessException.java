package basicAuth.exception;

/**
 * 首次访问异常
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class FirstTimeAccessException extends Exception {
    private static final long serialVersionUID = 4046517047810854244L;

    public FirstTimeAccessException(String message) {
        super(message);
    }
}
