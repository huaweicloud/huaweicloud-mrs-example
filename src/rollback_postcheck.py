#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
check after rollback
"""
import logging
from upgrade_postcheck import UpgradePostCheck
from log import Logger

LOG = logging.getLogger("zklog.rollback_postcheck")


class RollbackPostCheck(UpgradePostCheck):
    """
    init and recover zk
    """
    def __init__(self):
        super(RollbackPostCheck, self).__init__()
        self.find_crontab_cmd = "grep /opt/cloud/zookeeper/script/" \
                                "startZookeeper.sh /etc/crontab| wc -l"
        LOG.info("start roll back post check")


if __name__ == "__main__":
    Logger('zklog')
    post = RollbackPostCheck()
    post.do_post_check()
    post.print_exit()
