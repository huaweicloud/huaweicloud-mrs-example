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
import logging
from collections import OrderedDict

from log import Logger
from upgrade_precheck import linux_local_exec, ZK_ENV_PATH, ZK_CFG_PATH
from upgrade_pre import chown_recurse, UPGRADE_BACKUP_PATH

LOG = logging.getLogger("zklog.upgrade_post")
ZK_ZOO_DY_CFG_PATH = "/opt/cloud/zookeeper/conf/zoo.cfg.dynamic"
INIT_ZK = "/opt/cloud/zookeeper/script/config/init_zookeeper.py"
JAAS_FILE = "/opt/cloud/zookeeper/conf/server.jaas"


def fix_config():
    """fix configs in order to compact old version"""
    cmd = "sed -i 's/snapshot.trust.empty=false/snapshot.trust.empty=true/g' %s" % ZK_CFG_PATH
    err, out = linux_local_exec(cmd)
    if err:
        LOG.warning("fix snapshot.trust.empty failed: %s, %s", str(err), str(out))
        return False
    LOG.info("fix snapshot.trust.empty success")
    return True


def recover_sasl():
    """recover sasl user admin"""
    backup_jaas_path = "%s/conf/server.jaas" % UPGRADE_BACKUP_PATH
    cmd = "grep user_admin= %s |awk -F\\\" '{print $2}'|xargs sh " \
          "/opt/cloud/zookeeper/bin/zkSccTool.sh" % backup_jaas_path
    err, out = linux_local_exec(cmd)
    if err or not out:
        LOG.info("find jaas config failed: %s, %s", str(err), str(out))
        return "failed"
    sasl_str = 'user_admin="%s"' % out.strip()
    cmd = r"sed -i '/user_super/i\        %s' %s" % (sasl_str, JAAS_FILE)
    err, out = linux_local_exec(cmd)
    if err:
        LOG.info("write jaas config failed: %s, %s", str(err), str(out))
        return "failed"
    LOG.info("recover sasl config success")
    return "success"


class UpgradePost:
    """
    init and recover zk
    """
    def __init__(self):
        self.ips = []
        self.ids = OrderedDict()
        self.deploy_config = {
            "ssl": "",
            "sasl": "",
            "heap": "",
            "myid": "",
            "quorums": []
        }
        self.fail_count = 0
        self.result = {
            "code": 400,
            "msg": "failed",
            "data": []
        }
        LOG.info("init UpgradePost success")

    def do_post(self):
        """do actions after upgrade"""
        self.record_result("load config", self.load_config())
        self.record_result("init", self.init_zk())
        self.record_result("recover config", self.recover_config())
        if self.deploy_config["sasl"] == "true":
            self.record_result("recover sasl", recover_sasl())
        self.record_result("recover data", self.recover_data())
        chown_recurse("/opt/cloud/zookeeper", "service")

    def record_result(self, item, res):
        """record results"""
        if "failed" in res:
            self.fail_count += 1
        self.result["data"].append({'item_name': item, 'msg': res})
        LOG.info("result data update: %s", self.result["data"])

    def print_exit(self):
        """print and exit"""
        if not self.fail_count:
            self.result["code"] = 200
            self.result["msg"] = "success"
        print(json.dumps(self.result))
        sys.exit(0)

    def load_config(self):
        """load config from backup"""
        flags = os.O_RDONLY
        modes = stat.S_IWUSR | stat.S_IRUSR
        try:
            with os.fdopen(os.open(UPGRADE_BACKUP_PATH + "/deploy_config.json",
                                   flags, modes), 'r') as f:
                self.deploy_config = json.load(f)
            for server_str in self.deploy_config["quorums"]:
                server_ip = server_str.split("=")[1].split(":")[0]
                server_id = server_str.split("=")[0]
                self.ids.update({server_ip: server_id})
            self.ips = list(self.ids.keys())
            return "success"
        except (IOError, ValueError, IndexError) as e:
            LOG.warning("load json from deploy config failed: %s", str(e))
            return "failed, %s" % str(e)

    def init_zk(self):
        """Initialize zookeeper"""
        cmd = "python {0} -i {1}".format(INIT_ZK, ",".join(self.ips))
        if self.deploy_config["ssl"] == "false":
            cmd += " --unsafe"
        err, out = linux_local_exec(cmd)
        if err or not out:
            LOG.warning("init zk failed: %s, %s", str(err), str(out))
            return "failed, %s" % str(out)
        LOG.info("init zk success")
        return "success"

    def recover_config(self):
        """recover real id to dynamic config"""
        if not self._recover_ids():
            return "failed, recover ids failed"
        if not self._recover_heap():
            return "failed, recover heap failed"
        if not fix_config():
            return "failed, fix config failed"
        return "success"

    @staticmethod
    def recover_data():
        """recover data from backup"""
        try:
            if os.path.isdir("/opt/cloud/zookeeper/data/version-2"):
                shutil.rmtree("/opt/cloud/zookeeper/data/version-2")
            shutil.copytree("/opt/cloud/logs/zkBackup/data/version-2",
                            "/opt/cloud/zookeeper/data/version-2")
            LOG.info("revcover data success")
            return "success"
        except (IOError, OSError) as e:
            LOG.warning("recover data failed: %s", str(e))
            return "failed"

    def _recover_ids(self):
        """recover ids of quorums"""
        to_do_cmd = []
        for ip, real_id in self.ids.items():
            default_id = "server.%s" % self.ips.index(ip)
            if not real_id == default_id:
                to_do_cmd.append("sed -i 's/{0}={1}/{2}={1}/g' {3}".format(
                    default_id, ip, real_id, ZK_ZOO_DY_CFG_PATH))
        if not to_do_cmd:
            LOG.info("ids is standard")
            return True
        to_do_cmd.append("echo %s > /opt/cloud/zookeeper/data/myid" %
                         self.deploy_config["myid"])
        cmd = "; ".join(to_do_cmd)
        LOG.info("replace ids: %s", cmd)
        err, out = linux_local_exec(cmd)
        if err:
            LOG.warning("recover ids failed: %s, %s", str(err), str(out))
            return False
        LOG.info("recover ids success")
        return True

    def _recover_heap(self):
        """set zk heap size"""
        LOG.info("set java heap size to %s", self.deploy_config["heap"])
        try:
            with os.fdopen(os.open(ZK_ENV_PATH, os.O_RDONLY,
                                   stat.S_IWUSR | stat.S_IRUSR), "r") as f:
                tmp = f.read()
            heap_string = '\nZK_SERVER_HEAP="{0}"\n'.format(self.deploy_config["heap"])
            tmp = re.sub("\nZK_SERVER_HEAP.*\n", heap_string, tmp)
            LOG.info("replace heap str success, heap is %s", self.deploy_config["heap"])
            with os.fdopen(os.open(ZK_ENV_PATH, os.O_WRONLY | os.O_TRUNC,
                                   stat.S_IWUSR | stat.S_IRUSR), "w") as f:
                f.write(tmp)
            LOG.info("write heap str success")
            return True
        except (IOError, OSError) as e:
            LOG.warning("write java heap failed: %s", str(e))
            return False


if __name__ == "__main__":
    Logger('zklog')
    post = UpgradePost()
    post.do_post()
    post.print_exit()
