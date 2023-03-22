#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
check after upgrade
"""
import json
import re
import sys
import logging

from log import Logger
from upgrade_precheck import linux_local_exec, get_process_str
from upgrade_pre import get_host_ip, ls_nodes

LOG = logging.getLogger("zklog.upgrade_postcheck")


def check_role():
    """check role"""
    LOG.info("start to check role")
    err, out = linux_local_exec("source /etc/profile; "
                                "sh /opt/cloud/zookeeper/bin/zkServer.sh status")
    if err or ("Mode" not in out):
        LOG.warning("check role failed: %s, %s", err, out.strip())
        return "failed"
    LOG.info("check role success, %s", out.strip())
    stat_str = re.search("Mode: (.*)?", out).group(1)
    role = stat_str.strip()
    return "success, %s" % role


class UpgradePostCheck(object):
    """
    init and recover zk
    """
    def __init__(self):
        self.fail_count = 0
        self.result = {
            "code": 400,
            "msg": "failed",
            "data": []
        }
        self.find_crontab_cmd = "grep /opt/cloud/zookeeper/script/tools/" \
                                "start_zookeeper.py /etc/crontab| wc -l"
        LOG.info("init UpgradePostCheck success")

    def do_post_check(self):
        """check status after upgrade"""
        self.record_result("process", "success" if get_process_str() else "failed")
        self.record_result("crontab", self.check_crontab())
        self.record_result("role", check_role())
        self.record_result("cluster", ls_nodes(get_host_ip()))

    def check_crontab(self):
        """Check zookeeper crontab"""
        err, out = linux_local_exec(self.find_crontab_cmd)
        if err or not out:
            LOG.warning("get crontab failed: %s, %s", err, out)
            return "failed, get crontab failed"
        if out.strip() != "1":
            LOG.warning("crontab task count is %s", out.strip())
            return "failed, crontab task count is %s" % out.strip()
        LOG.info("check crontab success")
        return "success"

    def record_result(self, item, res):
        """record check result"""
        if "failed" in res:
            self.fail_count += 1
        self.result["data"].append({'item_name': item, 'msg': res})
        LOG.info("update result data: %s", self.result["data"])

    def print_exit(self):
        """print and exit"""
        if not self.fail_count:
            self.result["code"] = 200
            self.result["msg"] = "success"
        print(json.dumps(self.result))
        sys.exit(0)


if __name__ == "__main__":
    Logger('zklog')
    post = UpgradePostCheck()
    post.do_post_check()
    post.print_exit()
