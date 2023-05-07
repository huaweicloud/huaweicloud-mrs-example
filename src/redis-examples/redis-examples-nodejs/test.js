"use strict";

const stream = require("net");
const {genCommand} = require("./redisProtocol")
const Krb5 = require("./kerberosAuth")
const {initParser} = Krb5

let realm = "HADOOP.COM";
let username = "flinkuser" + "@" + realm;
// 初始化Krb5Auth
let auth = new Krb5.Krb5Auth("./krb5.conf", "./user.keytab", realm, username);
auth.setCCname("/srv/kerberos-auth/flinkuser_10000");
// 在认证之前需要先登录kerberos，有效时长24h，需要在过期时间内定时重新调用，保证登录不过期
auth.kinit();
let connectionOptions = {
    "port": 22400,
    "host": "192.168.64.138"
};
// 连接Redis服务
const client = stream.createConnection(connectionOptions);
// 初始化Redis返回值解析器
const authParser = initParser(auth);
// 开始认证
auth.authStart(client);
let count = 1;
client.on("data", (buffer) => {
    authParser.execute(buffer);
    if (auth.error) {
        console.log("error");
        return;
    }
    if (count <= 0) {
        // 认证通过后发送一次明后退出，无实际意义。
        process.exit();
    }
    if (auth.authed) {
        // 认证通过之后开始发送业务命令，此处以"config get maxmemory"为例
        console.log("send info");
        client.write(genCommand("config", ["get", "maxmemory"]));
        count--;
    } else {
        // 在没有认证通过前，拿到服务端ticket，进行校验，并且生成客户端ticket，直到服务端认证通过，此过程一般需要2-3次左右
        auth.authTicket(client);
    }
});
