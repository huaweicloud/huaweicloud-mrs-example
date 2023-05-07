"use strict";

const {execSync} = require("child_process");
const Parser = require("redis-parser");
const {writeAuthext} = require("./redisProtocol");
const krb5auth = require('bindings')('krb5auth');
const {initializeClient, genClientId, kerberosStart, kerberosStep, userNameGetter, isComplete} = krb5auth;

class Krb5Auth {

    /**
     * 构造函数需要kerberos认证相关的信息
     * @param krb5 krb5.conf，可以从认证凭据里面获取
     * @param keytab keytab文件，可以从认证凭据里面获取
     * @param realm 当前集群的域名，一般需要大写
     * @param username 用户名
     */
    constructor(krb5, keytab, realm, username) {
        this.krb5 = krb5;
        this.keytab = keytab;
        this.realm = realm;
        this.username = username;
        this.ccname = "/tmp/krb5_" + username;
        this.kinited = false;
        this.authed = false;
        this.serverTicket = Buffer.allocUnsafe(0);
        this.error = false;
    }

    /**
     * 设置kinit认证缓存路径，多个用户同时认证时，建议单独设置
     * @param ccname ccanme路径
     */
    setCCname(ccname) {
        this.ccname = ccname;
    }

    /**
     * 使用krb5.conf，user.keytab登录kerberos。
     */
    kinit() {
        process.env.KRB5_CONF = this.krb5;
        process.env.KRB5_KTNAME = this.keytab;
        process.env.KRB5CCNAME = this.ccname;
        let kinitCommand = "kinit -kt " + this.keytab + " " + this.username;
        let result = execSync(kinitCommand);
        if (result.length === 0) {
            console.log("kinited");
            return;
        }
        console.log(result);
    }

    /**
     * 开始认证
     * @param client Redis连接，可通过此连接发送命令
     */
    authStart(client) {
        if (this.started) {
            console.log("auth started")
            return;
        }
        initializeClient();
        this.clientId = genClientId();
        let clientTicket = kerberosStart("hadoop." + this.realm, this.clientId);
        let ctBase64 = Buffer.from(clientTicket, "base64");
        // 将产生的ticket通过authext命令发送给Redis服务端
        writeAuthext(client, ctBase64);
        this.started = true;
    }

    /**
     * 认证服务端token，并产生客户端ticket，将其发送给服务端，直到认证成功。
     * @param client Redis连接，可通过此连接发送命令
     */
    authTicket(client) {
        if (this.authed) {
            console.log("authed")
            return;
        }
        let stBase64 = this.serverTicket.toString('base64');
        let clientTicket = kerberosStep(stBase64, this.clientId, this.serverTicket.byteLength);
        if (clientTicket.length === 0) {
            writeAuthext(client, "");
            return;
        }
        let ctBase64 = Buffer.from(clientTicket, "base64");
        writeAuthext(client, ctBase64);
    }


}

function initParser(auth) {
    return new Parser({
        returnReply(reply) {
            if (auth.authed) {
                // 已经认证通过了，无需再解析ticket，直接打印返回内容
                console.log("-----------result begin-------------------");
                console.log(reply.toString());
                console.log("-----------result end-------------------");
                return;
            }
            if (reply.toString() === "authok") {
                // 如果服务端返回authok，说明认证已经通过，不需要客户端再产生ticket了。
                auth.authed = true;
                console.log("auth ok");
                return;
            }
            // 服务端返回格式：act:ticket内容，下面代码用于解析ticket
            let copyBuf = Buffer.allocUnsafe(reply.byteLength - 4);
            reply.copy(copyBuf, 0, 4, reply.byteLength);
            auth.serverTicket = copyBuf;
        },
        returnError(err) {
            console.log("------------error----------------");
            console.log(err);
            auth.error = true;
            console.log("------------error end----------------");
        },
        returnBuffers: true,
        stringNumbers: true
    });
}

module.exports = {
    Krb5Auth,
    initParser
}