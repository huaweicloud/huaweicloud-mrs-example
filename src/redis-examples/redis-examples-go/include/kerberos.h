#ifndef CKERBEROS_KERBEROS_H
#define CKERBEROS_KERBEROS_H

#include <sasl/sasl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#define APP_NAME  "redis"
#define AUTH_MECHANISM  "GSSAPI"

/**
 * 认证信息结构体
 */
struct client_state {
    sasl_conn_t *sasl_conn; // sasl连接
    int auth_started; // 认证是否已经开始
    int auth_complete; // 认证是否已经通过
    char *username; // 认证通过的用户名
};

/**
 * 认证中间过程返回结果信息
 */
typedef struct {
    int code; // 错误码，0：表示成功；-1：表示失败
    const char *message; // 错误信息
    int len; // ticket 长度
    char *response; // ticket 信息
} gss_result;

/**
 * 初始化客户端认证环境，需要在kinit之后、认证开始之前调用
 */
void initializeClient();

/**
 * 客户端认证开始时调用，每一次认证只能调用一次
 * @param full_server_realm 服务端域名，格式为hadoop.域名
 * @param state 认证状态信息
 * @return 启动认证的结果
 */
gss_result start(const char *full_server_realm, struct client_state *state);

/**
 * 认证过程中，校验服务端ticket，认证过程中可以多次调用，直到认证通过
 * @param challenge  服务端ticket
 * @param len 服务端ticket长度
 * @param state 认证状态信息
 * @return  认证结果
 */
gss_result step(const char *challenge, size_t len, struct client_state *state);

/**
 * 关闭当前认证信息
 * @param state
 */
void close_sasl(struct client_state *state);

#endif //CKERBEROS_KERBEROS_H
