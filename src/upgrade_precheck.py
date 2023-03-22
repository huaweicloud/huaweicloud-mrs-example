#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
check zookeeper status and configs before upgrade
"""
import json
import os
import stat
import sys
import re
import logging

from log import Logger

if sys.version_info.major == 2:
    import commands as linux_cmd
else:
    import subprocess as linux_cmd

LOG = logging.getLogger("zklog.upgrade_precheck")
ZK_CFG_PATH = "/opt/cloud/zookeeper/conf/zoo.cfg"
ZK_SERVER_PATH = "/opt/cloud/zookeeper/bin/zkServer.sh"
ZK_ENV_PATH = "/opt/cloud/zookeeper/bin/zkEnv.sh"
PARA_CONF_DICT = {"tickTime": "", "initLimit": "", "syncLimit": "", "clientPort": "2181",
                  "clientPortAddress": "", "snapCount": "",
                  "autopurge.snapRetainCount": "", "autopurge.purgeInterval": "",
                  "maxClientCnxns": "", "dataDir": "/opt/cloud/zookeeper/data",
                  "dataLogDir": "/opt/cloud/zookeeper/data", "gtszk": "",
                  "maxBackupNumber": "", "4lw.commands.whitelist": "",
                  "backupDir": ""}


def linux_local_exec(cmd):
    """
    function: execute linux command
    param: linux command
    return: command return status and value
    """
    status, output = linux_cmd.getstatusoutput(cmd)
    return status, output


def get_host_ip():
    """
    get host ip of eth0, not support multi network adapters
    """
    ifconfig_cmd = "ifconfig eth0 | grep inet | awk '{print $2}'|grep -v :"
    _, output = linux_local_exec(ifconfig_cmd)
    host_ip = output.strip()
    return host_ip


def get_process_str():
    """check process"""
    err, out = linux_local_exec("ps -wwef| grep QuorumPeerMain| grep -v grep| grep service")
    if err or not out:
        return ""
    return out.strip()


def get_myid():
    """
    get myid from file
    :return: myid
    """
    flags = os.O_RDONLY
    modes = stat.S_IWUSR | stat.S_IRUSR
    with os.fdopen(os.open("/opt/cloud/zookeeper/data/myid", flags, modes), 'r') as f:
        my_id = f.readline().strip()
    LOG.info("get myid: %s", my_id)
    return my_id


class UpgradePreCheck:
    """
    check if ready to upgrade
    """

    def __init__(self):
        self.ip = get_host_ip()
        self.myid = get_myid()
        self.detail_version = ""
        self.fail_count = 0
        self.process_str = ""
        self.server_str = {}
        self.result = {
            "code": 400,
            "msg": "failed",
            "data": []
        }
        if not os.path.isfile(ZK_CFG_PATH):
            LOG.warning("zoo.cfg is not a file")
            self.print_exit()
        self.get_version()
        LOG.info("init UpgradePreCheck success")

    def do_pre_check(self):
        """do pre check"""
        self.check_version()
        self.check_mode()
        self.check_sys()
        self.check_config()
        self.record_result("disk_free", self.check_disk_free())

    def record_result(self, item, res):
        """record execute result"""
        if "failed" in res:
            self.fail_count += 1
        self.result["data"].append({'item_name': item, 'msg': res})
        LOG.info("update data: %s", self.result["data"])

    def print_exit(self):
        """print and exit"""
        if not self.fail_count:
            self.result["code"] = 200
            self.result["msg"] = "success"
        print(json.dumps(self.result))
        sys.exit(0)

    def get_version(self):
        """Obtaining the RPM Package Version and Detailed Version"""
        status, out = linux_local_exec('rpm -qa| grep zookeeper')
        if not status:
            detail_ver = re.search("cloudmiddleware-zookeeper-(.*)-.*", out).group(1)
            detail_ver = detail_ver.split("_")[0]
            self.detail_version = detail_ver

    def check_mode(self):
        """check deploy mode"""
        cmd = r'grep -E "\{\{local_ip\}\}|cmw-1,cmw-2,cmw-3" /opt/root/zookeeper/conf/arb.cfg'
        err, out = linux_local_exec(cmd)
        if err or not out:
            LOG.warning("check mode failed: %s, %s", err, out.strip())
            self.record_result("mode", "failed, not support 2AZ deploy mode")
            return
        self.record_result("mode", "success")

    def check_version(self):
        """check if version is 3.4.14"""
        if not self.detail_version.startswith("3.4.14"):
            self.record_result("version", "failed, version is %s" % self.detail_version)
            return
        self.record_result("version", "success, version is %s" % self.detail_version)

    def check_sys(self):
        """
        check system environments and configs
        """
        self.process_str = get_process_str()
        self.record_result("user", "success" if self.process_str else "failed")
        self.record_result("lsblk", self._disk_partition())
        self.record_result("crontab", self._check_crontab())

    def check_config(self):
        """check configs"""
        self.record_result("cfg", self._check_cfg())
        self.record_result("myid", self._check_id())
        self.record_result("jvm", self._check_jvm())
        self.record_result("ssl", self._check_ssl())
        self.record_result("sasl", self._check_sasl())

    @staticmethod
    def check_disk_free():
        """
        Check whether the remaining capacity of /opt/cloud/logs is sufficient for backup.
        """
        cmd_disk_free = "df -k | grep /opt/cloud/logs | awk '{print $4}'"
        err, disk_free_str = linux_local_exec(cmd_disk_free)
        if err or not disk_free_str:
            return "failed, disk is not partitioned"
        cmd_data_size = "du -b --max-depth=0 /opt/cloud/zookeeper " \
                        "/opt/cloud/logs/zookeeper| awk '{sum += $1}; END{print sum}'"
        err, data_size_str = linux_local_exec(cmd_data_size)
        if err or not data_size_str:
            LOG.warning("get data size failed")
            return "failed, get data size failed"
        try:
            disk_free = int(disk_free_str.strip()) * 1024
            data_size = int(data_size_str.strip())
        except ValueError:
            LOG.warning("trans size to int failed: %s/disk_free, %s/data_size",
                        disk_free_str, data_size_str)
            return "failed, trans size ValueError"
        if disk_free <= 1.5 * data_size:
            return "failed, avail disk is not sufficient"
        return "success"

    def _check_cfg(self):
        """
        load config paras from file and compare with template
        """
        non_standard = {}
        miss_items = {}
        key_list = []
        flags = os.O_RDONLY
        modes = stat.S_IWUSR | stat.S_IRUSR
        try:
            with os.fdopen(os.open(ZK_CFG_PATH, flags, modes), 'r') as f:
                lines = f.readlines()
            for line in lines:
                if '#' in line or not line.strip():
                    continue
                line = line.strip("\n")
                LOG.info('get config line: %s', line)
                [k, v] = line.split("=", 1)
                if "server." in k:
                    self.server_str.update({k: v})
                    continue
                if k not in PARA_CONF_DICT:
                    LOG.info("match non-standard para: %s", k)
                    non_standard.update({k: v})
                    continue
                key_list.append(k)
                if PARA_CONF_DICT[k] and v != PARA_CONF_DICT[k]:
                    LOG.info("match non-standard para: %s", k)
                    non_standard.update({k: v})
        except (IOError, OSError, IndexError) as e:
            LOG.warning("check config failed: %s", str(e))
        for k, v in PARA_CONF_DICT.items():
            if k not in key_list:
                miss_items.update({k: "not set"})
        if non_standard:
            return "failed, non-stand items: %s" % json.dumps(non_standard)
        return "success, miss itens: %s" % json.dumps(miss_items)

    def _check_id(self):
        if self.ip in self.server_str["server.%s" % self.myid]:
            return "success"
        LOG.info("myid mismatch: id is %s, ip is %s, server str is %s",
                 self.myid, self.ip, self.server_str["server.%s" % self.myid])
        return "failed, my id not match to cfg"

    @staticmethod
    def _disk_partition():
        err, out = linux_local_exec("lsblk")
        if err or not out:
            LOG.warning("exec lsblk failed: %s, %s", str(err), str(out))
            return "failed, exec lsblk failed"
        if "/opt/cloud/logs" not in out:
            return "failed, not found /opt/cloud/logs"
        return "success"

    @staticmethod
    def _check_crontab():
        err, out = linux_local_exec("grep startZookeeper.sh /etc/crontab| wc -l")
        if err or not out:
            LOG.warning("get crontab failed: %s, %s", str(err), str(out))
            return "failed, get crontab failed"
        if out != "1":
            return "failed, crontab count is not 1"
        return "success"

    def _check_jvm(self):
        xmx_list = re.findall(r'(-Xmx[A-Za-z0-9]*)', self.process_str)
        if not xmx_list:
            LOG.warning("JVM heap not found in process")
            return "failed, JVM heap not found in process"
        cmd = "grep SERVER_JVMFLAGS= {}".format(ZK_SERVER_PATH)
        err, out = linux_local_exec(cmd)
        if err or not out:
            LOG.warning("find JVM heap in zkServer.sh failed")
            return "failed, JVM heap not found in file"
        if xmx_list[0] not in out:
            return "failed, JVM heap not match, %s/%s" % (xmx_list[0], out.strip())
        return "success, JVM config is %s" % out.strip().split("=")[1]

    def _check_ssl(self):
        cmd = "grep ZOONIOSWITCH=on %s" % ZK_ENV_PATH
        err, out = linux_local_exec(cmd)
        if err or not out:
            LOG.warning("find NIO switch in zkEnv.sh failed")
            return "failed, not find NIO switch in zkEnv.sh"
        process_nio = bool("NIOServerCnxnFactory" in self.process_str)
        file_nio = not bool("#" in out.strip())
        if process_nio and file_nio:
            LOG.warning("check ssl failed, ssl is closed")
            return "failed, NIO is enabled, only ssl is supported"
        if not process_nio and not file_nio:
            return "success, netty is enabled"
        return "failed, process and file config is not match"

    def _check_sasl(self):
        """
        Check whether the sec function is enabled.
        """
        jaas_path_list = re.findall(
            "-Djava.security.auth.login.config=(.*?.jaas)", self.process_str)
        process_jaas_path = jaas_path_list[0] if jaas_path_list else ""
        err, out = linux_local_exec("grep SERVER_JAAS_CONF %s" % ZK_ENV_PATH)
        if err or not out:
            if not process_jaas_path:
                return "success, sasl is closed"
            LOG.warning("failed, jaas path %s in process not found in zkEnv",
                        process_jaas_path)
            return "failed, jaas path %s in process not found in zkEnv" % process_jaas_path
        file_jaas_path = re.findall(
            "-Djava.security.auth.login.config=(.*?.jaas)", out.strip())[0]
        if process_jaas_path == file_jaas_path and os.path.isfile(file_jaas_path):
            cmd = "grep user_admin= %s" % file_jaas_path
            err, out = linux_local_exec(cmd)
            if err or not out:
                LOG.warning("failed, sasl user is not admin")
                return "failed, sasl user is not admin"
            return "success, sasl is open"
        LOG.warning("sasl mismatch or jaas not found, %s/process, %s/file",
                    process_jaas_path, file_jaas_path)
        return "failed, sasl info mismatch or jaas not found"


if __name__ == "__main__":
    Logger('zklog')
    pre_check = UpgradePreCheck()
    pre_check.do_pre_check()
    pre_check.print_exit()
