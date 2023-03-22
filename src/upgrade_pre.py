#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
backup and get ready for upgrade
"""
import json
import os
import re
import stat
import sys
import shutil
import pwd
import logging

from log import Logger
from upgrade_precheck import linux_local_exec, get_myid, get_process_str, \
    ZK_CFG_PATH, ZK_ENV_PATH, ZK_SERVER_PATH

LOG = logging.getLogger("zklog.upgrade_pre")

UPGRADE_BACKUP_PATH = "/opt/cloud/logs/zkUpgradeBackup"
BACKUP_DICT = {
    "/opt/cloud/zookeeper/data": "/opt/cloud/logs/zkBackup/data",
    "/opt/cloud/zookeeper/conf": UPGRADE_BACKUP_PATH + "/conf",
    "/opt/cloud/zookeeper/bin": UPGRADE_BACKUP_PATH + "/bin"
}


def get_host_ip():
    """
    get host ip of eth0, not support multi network adapters
    """
    ifconfig_cmd = "ifconfig eth0 | grep inet | awk '{print $2}'|grep -v :"
    _, output = linux_local_exec(ifconfig_cmd)
    return output.strip()


def sasl_enable():
    """get sasl config"""
    err, out = linux_local_exec("grep SERVER_JAAS_CONF %s" % ZK_ENV_PATH)
    if err or not out:
        LOG.info("SERVER_JAAS_CONF not found, sasl is closed")
        return False
    LOG.info("sasl is open")
    return True


def ssl_enable():
    """get ssl config"""
    cmd = "grep -E ^ZOONIOSWITCH=on %s" % ZK_ENV_PATH
    err, out = linux_local_exec(cmd)
    if err or not out:
        LOG.info("NIO switch is off, ssl is open")
        return True
    LOG.info("NIO switch is on, ssl is closed")
    return False


def get_quorums():
    """
    get ips from zoo.cfg
    """
    quorums = []
    flags = os.O_RDONLY
    modes = stat.S_IWUSR | stat.S_IRUSR
    with os.fdopen(os.open(ZK_CFG_PATH, flags, modes), 'r') as f:
        lines = f.readlines()
    for line in lines:
        if "#" in line:
            continue
        if "server." in line:
            quorums.append(line.strip())
    LOG.info("get quorums: %s", quorums)
    return quorums


def get_heap():
    """get jvm heap"""
    cmd = "grep SERVER_JVMFLAGS= {}".format(ZK_SERVER_PATH)
    err, out = linux_local_exec(cmd)
    if err or not out:
        LOG.warning("find JVM heap in zkServer.sh failed")
        return ""
    LOG.info("get heap str: %s", out.strip())
    return out.strip().split('"')[1]


def chown_recurse(dir_path, username):
    """
    function：change owner of a directory and its sub directories, files
    paras：directory path, user name, group name
    return：null
    """
    uid = pwd.getpwnam(username).pw_uid
    gid = pwd.getpwnam(username).pw_gid
    dir_tree = os.walk(dir_path)
    for parent, dirs, files in dir_tree:
        os.chown(parent, uid, gid)
        for d in dirs:
            os.chown(parent + r'/' + d, uid, gid)
        for f in files:
            os.chown(parent + r'/' + f, uid, gid)


def ls_nodes(target):
    """connect to another node and try get """
    cmd = "source /etc/profile; echo -e 'ls /\nquit' |" \
          "sh /opt/cloud/zookeeper/bin/zkCli.sh " \
          "-server %s:%s |grep zookeeper" % (target, "2181")
    LOG.info("start to check cluster")
    err, out = linux_local_exec(cmd)
    if not err and out and "zookeeper" in out:
        LOG.info("check cluster success: %s", out.strip())
        return "success"
    LOG.warning("check cluster failed: %s, %s", err, out.strip())
    return "failed: %s, %s" % (err, out.strip())


class UpgradePre:
    """
    backup configs and data, collect info
    """
    def __init__(self):
        self.server_str = ""
        self.deploy_config = {
            "ssl": "true",
            "sasl": "false",
            "heap": "",
            "myid": "-1",
            "quorums": [],
            "old_version": ""
        }
        self.fail_count = 0
        self.result = {
            "code": 400,
            "msg": "failed",
            "data": []
        }
        if get_process_str():
            self.record_result("process", "failed, process exist")
            self.print_exit()
        self.get_version()
        LOG.info("init UpgradePre success")

    def do_pre(self):
        """do pre actions before upgrade"""
        self.collect_config()
        self.backup_dirs()
        self.backup_log()
        chown_recurse("/opt/cloud/logs/zkUpgradeBackup", "service")
        chown_recurse("/opt/cloud/logs/zkBackup", "service")
        self.record_result("check cluster", self.check_cluster())

    def record_result(self, item, res):
        """record execute result"""
        if "failed" in res:
            self.fail_count += 1
        self.result["data"].append({'item_name': item, 'msg': res})

    def print_exit(self):
        """print and exit"""
        if not self.fail_count:
            self.result["code"] = 200
            self.result["msg"] = "success"
        print(json.dumps(self.result))
        sys.exit(0)

    def get_version(self):
        """Obtaining the RPM Detailed Version"""
        status, out = linux_local_exec('rpm -qa| grep zookeeper')
        if not status:
            detail_ver = re.search("cloudmiddleware-zookeeper-(.*)-.*", out).group(1)
            detail_ver = detail_ver.split("_")[0]
            self.deploy_config["old_version"] = detail_ver

    def collect_config(self):
        """get and save config"""
        self.deploy_config["ssl"] = "true" if ssl_enable() else "false"
        self.deploy_config["sasl"] = "true" if sasl_enable() else "false"
        self.deploy_config["heap"] = get_heap()
        self.deploy_config["myid"] = get_myid()
        self.deploy_config["quorums"] = get_quorums()
        for k, v in self.deploy_config.items():
            if not v:
                LOG.warning("get %s failed", k)
                self.record_result("get %s" % k, "failed")
                continue
            self.record_result("get %s" % k, "success, %s" % v)
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        modes = stat.S_IWUSR | stat.S_IRUSR
        try:
            with os.fdopen(os.open(
                    UPGRADE_BACKUP_PATH + "/deploy_config.json", flags, modes), "w") as f:
                json.dump(self.deploy_config, f)
            self.record_result("write config", "success")
        except IOError as e:
            LOG.warning("write deploy config failed, %s", e)
            self.record_result("write config", "failed")

    def backup_dirs(self):
        """backup dirs, include data/conf/bin"""
        try:
            for src, tar in BACKUP_DICT.items():
                if os.path.isdir(tar):
                    shutil.rmtree(tar)
                shutil.copytree(src, tar)
                LOG.info("copy dir success: %s", tar)
            self.record_result("backup dirs", "success")
        except (IOError, OSError) as e:
            LOG.warning("copy dir error: %s", str(e))
            self.record_result("backup dirs", "failed, %s" % str(e))

    def backup_log(self):
        """backup last 10 logs"""
        if os.path.isdir(UPGRADE_BACKUP_PATH + "/log"):
            shutil.rmtree(UPGRADE_BACKUP_PATH + "/log")
        os.mkdir("/opt/cloud/logs/zkUpgradeBackup/log", 0o700)
        cmd = "ls -tr /opt/cloud/logs/zookeeper/*.log*| tail -5 |xargs -i " \
              "cp -f {} %s" % UPGRADE_BACKUP_PATH + "/log/"
        LOG.info("start to backup logs")
        err, out = linux_local_exec(cmd)
        if err:
            LOG.warning("backup logs failed: {}", str(out).strip())
            self.record_result("backup logs", "failed")
            return
        self.record_result("backup logs", "success")

    def check_cluster(self):
        """connect to another node and try get """
        host_ip = get_host_ip()
        target = ""
        fail_res = "failed, please check cluster state manually"
        try:
            for peer in self.deploy_config["quorums"]:
                if host_ip not in peer:
                    target = peer.split("=")[1].split(":")[0]
                    break
        except IndexError:
            LOG.warning("cannot find a target to check cluster")
            return fail_res
        if not target:
            LOG.warning("cannot find a target to check cluster")
            return fail_res
        return ls_nodes(target)


if __name__ == "__main__":
    Logger('zklog')
    pre = UpgradePre()
    pre.do_pre()
    pre.print_exit()
