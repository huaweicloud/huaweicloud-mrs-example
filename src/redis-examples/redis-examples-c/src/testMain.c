#include <stdio.h>
#include "kerberos.h"
#include "hiredis/hiredis.h"
#include "hiredis/sds.h"



#define CRLF            "\r\n"
#define AUTH_OK         "authok"
// auth continue
#define ACT_PREFIX      "act:"
#define CRLF_LEN        2
#define AUTH_OK_LEN     6
#define ACT_PREFIX_LEN  4

/* Error codes */
#define C_OK                    0
#define C_ERR                   -1

int sds2str(char *s, long long value) {
    char *p, aux;
    unsigned long long v;
    size_t l;

    /* Generate the string representation, this method produces
     * a reversed string. */
    v = (value < 0) ? -value : value;
    p = s;
    do {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);
    if (value < 0) *p++ = '-';

    /* Compute length and add null term. */
    l = p-s;
    *p = '\0';

    /* Reverse the string. */
    p--;
    while(s < p) {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

/**
 * 发送认证信息到服务端
 * @param c Redis连接信息
 * @param datalen ticket长度
 * @param data ticket内容
 * @return 是否发送成功
 */
int sendAuthReq(redisContext *c, size_t datalen, char *data) {
    sds cmd = sdsnew("*2\r\n$7\r\nauthext\r\n");
    char buf[32] = {0};
    long len = 0;

    len = sds2str(buf, (long long )datalen);
    cmd = sdscatlen(cmd, "$", 1);
    cmd = sdscatlen(cmd, buf, len);
    cmd = sdscatlen(cmd, CRLF, CRLF_LEN);
    cmd = sdscatlen(cmd, data, datalen);
    cmd = sdscatlen(cmd, CRLF, CRLF_LEN);
    if (redisAppendFormattedCommand(c, cmd, sdslen(cmd)) != REDIS_OK) {
        return C_ERR;
    }
    return C_OK;
}

/**
 * 获取认证的返回信息
 * @param c Redis连接信息
 * @return 认证返回的ticket信息
 */
sds getAuthReply(redisContext *c) {
    redisReply *reply;
    if (redisGetReply(c, (void **)(&reply)) == REDIS_ERR) {
        printf("reply error\n");
        return NULL;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        printf("(error) %s\n", reply->str);
        freeReplyObject(reply);
        return NULL;
    }
    sds ticket = sdsnewlen(reply->str, reply->len);
    if (!strncmp(ticket, AUTH_OK, AUTH_OK_LEN)) {
        return ticket;
    }
    // 剔除服务端返回的无用的头信息: act:
    sdsrange(ticket, ACT_PREFIX_LEN, -1);
    freeReplyObject(reply);
    return ticket;
}


int main() {
    // 客户端启动时，初始化kerberos认证信息
    initializeClient();
    // 连接Redis，ip 端口可根据实际情况填写
    redisContext *c = redisConnect("192.168.67.125", 22406);
    int finished = 0;
    char* full_server_realm = "hadoop.HADOOP.COM" ;
    printf("full_server_realm=%s\n", full_server_realm);
    struct client_state state = {
            .auth_started = 0,
            .auth_complete = 0
    };
    // 开始认证
    gss_result result = start(full_server_realm, &state);
    if (result.code != 0) {
        printf("error:%s", result.message);
        return -1;
    }
    while (!finished) {
        sendAuthReq(c, result.len, result.response);
        sds reply = getAuthReply(c);
        if (reply == NULL) {
            return -1;
        }
        if (!strncmp(reply, AUTH_OK, AUTH_OK_LEN)) {
            finished = 1;
            continue;
        }
        result = step(reply, sdslen(reply), &state);
        if (result.code == -1) {
            printf("error:%s\n", result.message);
            return -1;
        }
        if (result.response == NULL) {
            result.response = "";
        }
    }

    printf("auth ok%s\n\n");
    printf("authed user=%s\n\n", state.username);
    redisCommand(c, "set %s %s", "b", "bbbbbb");
    redisReply *reply = redisCommand(c, "get %s", "b");
    printf("b=%s\n", reply->str);
    return 0;
}