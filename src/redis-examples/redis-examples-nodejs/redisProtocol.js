"use strict";

function writeStrParams(client, param) {
    client.write("$");
    writeIntCrlf(client, param.length);
    client.write(param + "\r\n");
}

function writeBufferParams(client, buffer) {
    client.write("$");
    writeIntCrlf(client, buffer.length);
    client.write(buffer);
    client.write("\r\n");
}

function writeIntCrlf(client, len) {
    client.write(len + "\r\n");
}

function writeAuthext(client, ticket) {
    client.write("*");
    writeIntCrlf(client, 2);
    writeStrParams(client, "authext");
    writeBufferParams(client, ticket);
}

function genCommand(command, args) {
    let cmd = "*" + (args.length + 1) + "\r\n";
    cmd += "$" + command.length + "\r\n" + command + "\r\n";
    for (let i = 0; i < args.length; i++) {
        cmd += "$" + args[i].length + "\r\n" + args[i] + "\r\n";
    }
    return cmd;
}

module.exports = {
    writeAuthext,
    genCommand
}