package basicAuth.exception;

/**
 * 无效的输入异常
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class InvalidInputParamException extends Exception {
    private static final long serialVersionUID = 4046517047810854242L;

    public InvalidInputParamException(String message) {
        super(message);
    }
}
