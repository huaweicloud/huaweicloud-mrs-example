#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
pre check for rollback
"""
import json
import sys
import logging
from log import Logger

LOG = logging.getLogger("zklog.rollback_pre")


class RollbackPre:
    """
    prepare to rollback
    """
    def __init__(self):
        self.fail_count = 0
        self.result = {
            "code": 400,
            "msg": "failed",
            "data": []
        }

    def do_rollback_pre(self):
        self.record_result("skip", "success")

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


if __name__ == "__main__":
    Logger('zklog')
    rc = RollbackPre()
    rc.do_rollback_pre()
    rc.print_exit()
