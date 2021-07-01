package validutil;

import org.apache.commons.lang3.StringUtils;

/**
 * ParamsValidUtil
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class ParamsValidUtil {
    /**
     * 判断是否有为空的参数
     *
     * @param obj 输入的对象集
     * @return 是否有为空的对象
     * @exception/throws [违例类型] [违例说明]
     * @see [类、类#方法、类#成员]
     */
    public static boolean isNull(Object... obj) {
        if (obj == null) {
            return true;
        }

        for (int i = 0; i < obj.length; i++) {
            if (obj[i] == null) {
                return true;
            }
        }

        return false;
    }

    /**
     * 判断是否有为空的参数
     *
     * @param obj 输入的参数集
     * @return 是否有为空的参数
     * @exception/throws [违例类型] [违例说明]
     * @see [类、类#方法、类#成员]
     */
    public static boolean isEmpty(String... obj) {
        for (int i = 0; i < obj.length; i++) {
            if (StringUtils.isEmpty(obj[i])) {
                return true;
            }
        }
        return false;
    }
}
