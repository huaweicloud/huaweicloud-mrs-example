'use strict';

const krb5auth = require("bindings")("krb5auth");

const protocol = require("./redisProtocol");

const Krb5 = require("./kerberosAuth")

module.exports = {
    krb5auth,
    protocol,
    Krb5
}

module.exports.version = require('./package.json').version;
