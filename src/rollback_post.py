#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
backup and get ready for upgrade
"""
import json
import os
import shutil
import sys
import logging

from log import Logger
from upgrade_precheck import get_process_str, linux_local_exec
from upgrade_pre import chown_recurse, BACKUP_DICT
from rollback_precheck import get_old_version

LOG = logging.getLogger("zklog.rollback_post")


def add_crontab():
    write_cmd = "echo '#*/1 * * * * service sh /opt/cloud/zookeeper/" \
                "script/startZookeeper.sh &> /dev/null' >> /etc/crontab"
    err, out = linux_local_exec(write_cmd)
    if err:
        LOG.warning("add crontab failed, %s, %s" % (err, out))
        return "failed, %s" % out
    LOG.info("add crontab success")
    return "success"


class RollbackPost:
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
        self.old_version = get_old_version()
        if get_process_str():
            self.record_result("process", "failed, process exist")
            self.print_exit()

    def do_rollback(self):
        """copy backup dirs to origin path"""
        LOG.info("start to recover backup dirs")
        try:
            for src, tar in BACKUP_DICT.items():
                if os.path.isdir(src):
                    shutil.rmtree(src)
                shutil.copytree(tar, src)
                chown_recurse(src, "service")
            self.record_result("recover dirs", "success")
        except (IOError, OSError) as e:
            LOG.warning("copy dir error: %s", str(e))
            self.record_result("recover dirs", "failed, %s" % str(e))
        if "3.4.14" in self.old_version:
            LOG.info("old version is 3.4.14, try to add crontab")
            self.record_result("add crontab", add_crontab())

    def record_result(self, item, res):
        """record result"""
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


if __name__ == "__main__":
    Logger('zklog')
    rc = RollbackPost()
    rc.do_rollback()
    rc.print_exit()
