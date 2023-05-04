#ifndef __AUTHENTICATION_H__
#define __AUTHENTICATION_H__
#include <sasl/sasl.h>
#include <napi.h>
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <stddef.h>

#define APP_NAME  "redis"
#define AUTH_MECHANISM  "GSSAPI"

static constexpr char CCH[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";


struct client_state {
    sasl_conn_t *sasl_conn;
    int auth_started;
    bool auth_complete;
    char* username;
};

typedef struct {
    int code;
    std::string message;
    unsigned len;
    char* response;
} gss_result;

static int sz = 25;

std::map<std::string, client_state> clientStateMap;

// 用于对ticket进行base64加密
char* base64_encode(const unsigned char* value, size_t vlen);
// 用于对ticket进行base64解密
unsigned char* base64_decode(const char* value, size_t* rlen);
// 初始化客户端认证环境，需要在kinit之后、认证开始之前调用
void initializeClient(const Napi::CallbackInfo& info);
// 客户端认证开始时调用，每一次认证只能调用一次
Napi::String kerberosStart(const Napi::CallbackInfo& info);
// 认证过程中，校验服务端ticket，认证过程中可以多次调用，直到认证通过
Napi::String kerberosStep(const Napi::CallbackInfo& info);
// 产生随机的clientId，用户识别当前认证
Napi::String genClientId(const Napi::CallbackInfo& info);

#endif
