#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
backup and get ready for upgrade
"""
import json
import os
import stat
import sys
import logging

from log import Logger
from upgrade_precheck import linux_local_exec
from upgrade_pre import UPGRADE_BACKUP_PATH

LOG = logging.getLogger("zklog.rollback_precheck")
BACKUP_LIST = [
    "/opt/cloud/logs/zkBackup/data",
    UPGRADE_BACKUP_PATH + "/conf",
    UPGRADE_BACKUP_PATH + "/bin"
]


def get_old_version():
    """get version before upgrade"""
    flags = os.O_RDONLY
    modes = stat.S_IWUSR | stat.S_IRUSR
    try:
        with os.fdopen(os.open(UPGRADE_BACKUP_PATH + "/deploy_config.json",
                               flags, modes), 'r') as f:
            deploy_config = json.load(f)
        return deploy_config["old_version"]
    except (IOError, ValueError, IndexError) as e:
        LOG.warning("load json from deploy config failed: %s", str(e))
        return ""


class RollbackPreCheck:
    """
    check if ready to rollback
    """
    def __init__(self):
        self.fail_count = 0
        self.result = {
            "code": 400,
            "msg": "failed",
            "data": []
        }
        self.old_vresion = get_old_version()
        if not self.old_vresion:
            self.record_result("old version", "failed")
            self.print_exit()

    def do_rollback_check(self):
        """check if can rollback"""
        self.record_result("rpm", self.check_rpm())
        self.record_result("backup", self.check_backup())

    def record_result(self, item, res):
        """record check result"""
        if "failed" in res:
            self.fail_count += 1
        self.result["data"].append({'item_name': item, 'msg': res})

    def print_exit(self):
        """print result and exit"""
        if not self.fail_count:
            self.result["code"] = 200
            self.result["msg"] = "success"
        print(json.dumps(self.result))
        sys.exit(0)

    def check_rpm(self):
        """check if rpm exist"""
        cmd = "ls /root/ |grep cloudmiddleware-zookeeper |grep %s" % self.old_vresion
        err, out = linux_local_exec(cmd)
        if err or not out:
            LOG.warning("not found rpm of old version: %s, %s", str(err), str(out))
            return "failed"
        return "success"

    @staticmethod
    def check_backup():
        """check if backup exist"""
        for backup_dir in BACKUP_LIST:
            if not os.path.isdir(backup_dir):
                return "failed, %s not exist" % backup_dir
        return "success"


if __name__ == "__main__":
    Logger('zklog')
    rc = RollbackPreCheck()
    rc.do_rollback_check()
    rc.print_exit()
