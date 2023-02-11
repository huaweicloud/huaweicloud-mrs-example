package com.huawei.dap.alb.plugins.impl;

import com.huawei.dap.alb.plugins.AccessResolveBean;
import com.huawei.dap.alb.plugins.HttpPluginsChain;
import com.huawei.dap.alb.plugins.HttpResolveItf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

/**
 * <p>实现resolve方法，从请求中取得serviceName、serviceVersion、MethodPath、httpMethod、queryString、entity并
 * 存入reqBean中，插件的工作就算完成了，怎么从请求中取出这些信息方式并没有限制，这里是一种针对请求为如下形式时
 * 的一种示例实现：</p>
 *       <p>http方法： http方法要和所请求的服务相应的方法注解的http方法相同</p>
 *       <p>head: 取决于实际场景添加参数，比如body中传递了json对象那么这里就要加入Content-Type: application/json，
 *             安全模式下要加auth_username和auth_password</p>
 *       <p>URL : http://ip:port/[serviceName]/[serviceVersion]/[methodPath]?[queryString]</p>
 *       <p>body: 所调用的方法要求通过body传递参数则在body中传递参数</p>
 * 本示例的实现方案是通过在请求URL中定位serviceVersion（此处版本号只考虑三段数字或星号）后根据相对位置取出serviceName、
 * methodPath等信息
 */
public class UserHttpResolvePlugin implements HttpResolveItf {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserHttpResolvePlugin.class);

    private static final String regexForVersion = "(/\\d+\\.\\d+\\.\\d+)|(\\*)";
    private static final Pattern patternForVersion = Pattern.compile(regexForVersion);

    /**
     * {@inheritDoc}
     *
     * @throws IOException
     */
    @Override
    public void resolve(
            HttpServletRequest request,
            HttpServletResponse response,
            HttpPluginsChain pluginsChain,
            AccessResolveBean reqBean)
            throws IOException {
        String pathInfo = request.getPathInfo();

        Matcher matcherForVersion = patternForVersion.matcher(pathInfo);
        if (matcherForVersion.find()) {
            String firstMatch = matcherForVersion.group();
            reqBean.setServiceName(pathInfo.substring(0, matcherForVersion.start()));
            reqBean.setServiceVersion(firstMatch.substring(1));
            reqBean.setMethodPath(pathInfo.substring(matcherForVersion.end(0)));
            reqBean.setHttpMethod(request.getMethod());
            reqBean.setQueryString(request.getQueryString());
            reqBean.setEntity(getEntity(request));
            LOGGER.debug(reqBean.toLiteString());
        } else {
            LOGGER.warn("[DefaultHttpResolvePlugin]" + request.getRequestURL() + " can not be resolved.");
        }
    }

    private Entity<?> getEntity(HttpServletRequest request) throws IOException {
        Entity<?> entity;

        String contentType = request.getContentType();
        if (contentType == null) {
            contentType = MediaType.APPLICATION_JSON;
        }

        entity = Entity.entity((InputStream) request.getInputStream(), contentType);

        return entity;
    }
}
