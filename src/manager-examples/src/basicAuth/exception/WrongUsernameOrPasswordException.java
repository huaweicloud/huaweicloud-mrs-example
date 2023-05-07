package basicAuth.exception;

/**
 * 错误的用户名或密码异常
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class WrongUsernameOrPasswordException extends Exception {
    private static final long serialVersionUID = 4046517047810854246L;

    public WrongUsernameOrPasswordException(String message) {
        super(message);
    }
}
