#!/usr/bin/python
# coding: utf-8
"""Script for logging"""
import logging
import logging.handlers
import os

LOG_FORMAT = '%(asctime)s-%(name)s-PID:%(process)d [%(filename)s - ' \
             'line:%(lineno)d] - %(levelname)s: %(message)s'


class Logger:
    """
    logger
    """
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    def __init__(self, name, level='info'):
        """
        **kwargs:back_count，new_log，console_print
        """
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.format_str = logging.Formatter(LOG_FORMAT)
        if not os.path.isdir("/opt/cloud/logs/zkUpgradeBackup"):
            os.mkdir("/opt/cloud/logs/zkUpgradeBackup", 0o700)
        self.log_dir = "/opt/cloud/logs/zkUpgradeBackup/zk_upgrade.log"
        self.logger.setLevel(self.level_relations.get(level))
        self.max_bytes = 20 * 1024 * 1024
        self.backup_count = 3
        self.file_handle_log_init(console_print=False, new_log=False)

    def file_handle_log_init(self, console_print, new_log):
        """
        No handlers or new log init
        """
        if not self.logger.handlers or new_log:
            file_handle = logging.handlers.RotatingFileHandler(
                filename=self.log_dir,
                backupCount=self.backup_count,
                maxBytes=self.max_bytes,
                encoding='utf-8')
            file_handle.setFormatter(self.format_str)
            self.logger.addHandler(file_handle)
            if console_print:
                console_handle = logging.StreamHandler()
                console_handle.setFormatter(self.format_str)
                self.logger.addHandler(console_handle)

    def get_log_dir(self):
        """
        get_log_dir
        """
        return self.log_dir
